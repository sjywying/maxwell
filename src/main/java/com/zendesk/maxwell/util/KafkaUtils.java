package com.zendesk.maxwell.util;

/**
 * Created by crazyy on 17/6/8.
 */
public class KafkaUtils {

	public static final String default_topic = "mysql_binlog_default";

	public static String getTopicKey(String database, String table) {

		if(database == null || table == null || "".equals(database) || "".equals(table)) {
			throw new RuntimeException("database or table name is null, database:"+database+", table:"+table);
		}

		return database + "_" + table;
	}
}
