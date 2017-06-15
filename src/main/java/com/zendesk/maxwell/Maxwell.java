package com.zendesk.maxwell;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.metrics.MaxwellMetrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.recovery.Recovery;
import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.replication.BinlogConnectorReplicator;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.MaxwellReplicator;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.util.KafkaUtils;
import com.zendesk.maxwell.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class Maxwell implements Runnable {
	static {
		Logging.setupLogBridging();
	}

	protected MaxwellConfig config;
	protected MaxwellContext context;
	protected Replicator replicator;

	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	public Maxwell(MaxwellConfig config) throws SQLException {
		this.config = config;
		this.context = new MaxwellContext(this.config);
		this.context.probeConnections();
	}

	public void run() {
		try {
			start();
		} catch (Exception e) {
			LOGGER.error("maxwell encountered an exception", e);
		}
	}

	public void terminate() {
		this.context.terminate();
	}

	private Position attemptMasterRecovery() throws Exception {
		Position recoveredPosition = null;
		MysqlPositionStore positionStore = this.context.getPositionStore();
		RecoveryInfo recoveryInfo = positionStore.getRecoveryInfo(config);

		if ( recoveryInfo != null ) {
			Recovery masterRecovery = new Recovery(
				config.replicationMysql,
				config.databaseName,
				this.context.getReplicationConnectionPool(),
				this.context.getCaseSensitivity(),
				recoveryInfo,
				this.config.shykoMode,
				this.context
			);

			recoveredPosition = masterRecovery.recover();

			if (recoveredPosition != null) {
				// load up the schema from the recovery position and chain it into the
				// new server_id
				MysqlSchemaStore oldServerSchemaStore = new MysqlSchemaStore(
					context.getMaxwellConnectionPool(),
					context.getReplicationConnectionPool(),
					context.getSchemaConnectionPool(),
					recoveryInfo.serverID,
					recoveryInfo.position,
					context.getCaseSensitivity(),
					config.filter,
					false
				);

				oldServerSchemaStore.clone(context.getServerID(), recoveredPosition);

				positionStore.delete(recoveryInfo.serverID, recoveryInfo.clientID, recoveryInfo.position);
			}
		}
		return recoveredPosition;
	}

	protected Position getInitialPosition() throws Exception {
		/* first method:  do we have a stored position for this server? */
		Position initial = this.context.getInitialPosition();

		if (initial == null) {

			/* second method: are we recovering from a master swap? */
			if ( config.masterRecovery )
				initial = attemptMasterRecovery();

			/* third method: capture the current master position. */
			if ( initial == null ) {
				try ( Connection c = context.getReplicationConnection() ) {
					initial = Position.capture(c, config.gtidMode);
				}
			}

			if (initial != null) {
				/* if the initial position didn't come from the store, store it */
				context.getPositionStore().set(initial);
			}
		}

		return initial;
	}

	public String getMaxwellVersion() {
		String packageVersion = getClass().getPackage().getImplementationVersion();
		if ( packageVersion == null )
			return "??";
		else
			return packageVersion;
	}

	static String bootString = "Maxwell v%s is booting (%s), starting at %s";
	private void logBanner(AbstractProducer producer, Position initialPosition) {
		String producerName = producer.getClass().getSimpleName();
		LOGGER.info(String.format(bootString, getMaxwellVersion(), producerName, initialPosition.toString()));
	}

	protected void onReplicatorStart() {}
	private void start() throws Exception {
		try {
			startInner();
		} catch ( Exception e) {
			this.context.terminate(e);
		} finally {
			this.context.terminate();
		}

		Exception error = this.context.getError();
		if (error != null) {
			throw error;
		}
	}

	private void startInner() throws Exception {
		try ( Connection connection = this.context.getReplicationConnection();
		      Connection rawConnection = this.context.getRawMaxwellConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);
			if (config.gtidMode) {
				MaxwellMysqlStatus.ensureGtidMysqlState(connection);
			}

			SchemaStoreSchema.ensureMaxwellSchema(rawConnection, this.config.databaseName);

			try ( Connection schemaConnection = this.context.getMaxwellConnection() ) {
				SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
			}
		}

		AbstractProducer producer = this.context.getProducer();
		AbstractBootstrapper bootstrapper = this.context.getBootstrapper();

		if( null != this.config.outputMergeColumns && !"".equals(this.config.outputMergeColumns)) {
			Map<String, Map<String, List<String>>> mergeColumns = new HashMap<>();
			//output_merge_columns=start_coordinate:STARTPOSITIONX,STARTPOSITIONY;end_coordinate:ENDPOSITIONX,ENDPOSITIONY
			String[] columnsKVs = this.config.outputMergeColumns.split(";");
			for (int i = 0; i < columnsKVs.length; i++) {
				String[] columnsKVArr = columnsKVs[i].split(":");
				List<String> columnsV = Arrays.asList(columnsKVArr[2].split(","));

				if(mergeColumns.containsKey(columnsKVArr[0])) {
					mergeColumns.get(columnsKVArr[0]).put(columnsKVArr[1], columnsV);
				} else {
					Map<String, List<String>> mergeColumn = new HashMap<>();
					mergeColumn.put(columnsKVArr[1], columnsV);
					mergeColumns.put(columnsKVArr[0], mergeColumn);
				}
			}

			this.context.setMergeColumns(mergeColumns);
		}

		Position initPosition = getInitialPosition();
		logBanner(producer, initPosition);
		this.context.setPosition(initPosition);

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, initPosition);
		Schema schema = mysqlSchemaStore.getSchema(); // trigger schema to load / capture before we start the replicator.

		if("kafka".equals(this.context.getConfig().producerType) && "*".equals(this.context.getConfig().kafkaTopic)) {
			//key ${databasename}_{tablename}, value topicname
			final Map<String, String> tableTopic = new HashMap<String, String>();

			for (Iterator<Database> iterator = schema.getDatabases().iterator(); iterator.hasNext(); ) {
				Database database = iterator.next();

				for (Iterator<Table> iterator1 = database.getTableList().iterator(); iterator1.hasNext(); ) {
					Table table = iterator1.next();

					MaxwellFilter.matches(context.getFilter(), table.getDatabase(), table.getName());

					tableTopic.put(KafkaUtils.getTopicKey(table.getDatabase(), table.getName()), table.getTopicName());
				}
			}

			context.setTableTopic(tableTopic);
		}

		if ( this.config.shykoMode )
			this.replicator = new BinlogConnectorReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, initPosition);
		else
			this.replicator = new MaxwellReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, initPosition);

		bootstrapper.resume(producer, replicator);

		replicator.setFilter(context.getFilter());

		context.setReplicator(replicator);
		this.context.start();
		this.onReplicatorStart();

		replicator.runLoop();
	}

	public static void main(String[] args) {
		try {
			MaxwellConfig config = new MaxwellConfig(args);

			if ( config.log_level != null )
				Logging.setLevel(config.log_level);

			final Maxwell maxwell = new Maxwell(config);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					maxwell.terminate();
					StaticShutdownCallbackRegistry.invoke();
				}
			});

			maxwell.start();
		} catch ( SQLException e ) {
			e.printStackTrace();
			// catch SQLException explicitly because we likely don't care about the stacktrace
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			LOGGER.error(e.getLocalizedMessage());
			System.exit(1);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
