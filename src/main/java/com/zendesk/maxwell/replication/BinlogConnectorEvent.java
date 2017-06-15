package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.*;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.util.MurmurHash3;

import java.io.Serializable;
import java.util.*;

public class BinlogConnectorEvent {
	private BinlogPosition position;
	private BinlogPosition nextPosition;
	private final Event event;
	private final String gtidSetStr;
	private final String gtid;
	//tablename, afterMergeName, list merge_column
	private final Map<String, Map<String, List<String>>> mergeColumns;
	private final Set<String> columns = new HashSet<>();

	public BinlogConnectorEvent(Event event, String filename, String gtidSetStr, String gtid, Map<String, Map<String, List<String>>> mergeColumns) {
		this.event = event;
		this.gtidSetStr = gtidSetStr;
		this.gtid = gtid;
		EventHeaderV4 hV4 = (EventHeaderV4) event.getHeader();
		this.nextPosition = new BinlogPosition(gtidSetStr, gtid, hV4.getNextPosition(), filename);
		this.position = new BinlogPosition(gtidSetStr, gtid, hV4.getPosition(), filename);
		this.mergeColumns = mergeColumns;

		if(mergeColumns != null && mergeColumns.size() > 0) {
			for (Iterator<String> iterator = mergeColumns.keySet().iterator(); iterator.hasNext(); ) {
				Map<String, List<String>> next = mergeColumns.get(iterator.next());
				if(next != null && !next.isEmpty()) {
					for (Iterator<String> iterator1 = next.keySet().iterator(); iterator1.hasNext(); ) {
						List<String> strings = next.get(iterator1.next());
						if(strings != null && !strings.isEmpty()) {
							for (Iterator<String> stringIterator = strings.iterator(); stringIterator.hasNext(); ) {
								String s = stringIterator.next();
								columns.add(s);
							}
						}
					}
				}
			}
		}
	}

	public Event getEvent() {
		return event;
	}

	public WriteRowsEventData writeRowsData() {
		return (WriteRowsEventData) event.getData();
	}

	public UpdateRowsEventData updateRowsData() {
		return (UpdateRowsEventData) event.getData();
	}

	public DeleteRowsEventData deleteRowsData() {
		return (DeleteRowsEventData) event.getData();
	}

	public QueryEventData queryData() {
		return (QueryEventData) event.getData();
	}

	public XidEventData xidData() {
		return (XidEventData) event.getData();
	}

	public TableMapEventData tableMapData() {
		return (TableMapEventData) event.getData();
	}

	public BinlogPosition getPosition() {
		return position;
	}

	public EventType getType() {
		return event.getHeader().getEventType();
	}

	public Long getTableID() {
		EventData data = event.getData();
		switch ( event.getHeader().getEventType() ) {
			case EXT_WRITE_ROWS:
			case WRITE_ROWS:
				return ((WriteRowsEventData) data).getTableId();
			case EXT_UPDATE_ROWS:
			case UPDATE_ROWS:
				return ((UpdateRowsEventData) data).getTableId();
			case EXT_DELETE_ROWS:
			case DELETE_ROWS:
				return ((DeleteRowsEventData) data).getTableId();
			case TABLE_MAP:
				return ((TableMapEventData) data).getTableId();
		}
		return null;
	}

	private void writeData(String type, Table table, RowMap row, Serializable[] data, BitSet includedColumns) {
		int dataIdx = 0, colIdx = 0;
		StringBuffer ctypeSB = new StringBuffer();
		Map<String, String> kv = new HashMap<>();

		for ( ColumnDef cd : table.getColumnList() ) {
			if ( includedColumns.get(colIdx) ) {

				//this.writeOldData()处理
				if(!"update".equals(type)) {
					ctypeSB.append(cd.getName()).append("_");
				}

				Object json = null;
				if ( data[dataIdx] != null ) {
					json = cd.asJSON(data[dataIdx]);
				}
				row.putData(cd.getName(), json);


				if(columns.contains(cd.getName())) {
					kv.put(cd.getName(), json == null ? "":json.toString());
				}

				dataIdx++;
			}
			colIdx++;
		}

		//merge columns
		if(mergeColumns.containsKey(table.getName())) {
			Map<String, List<String>> mergecolums = mergeColumns.get(table.getName());
			for (Iterator<String> iterator = mergecolums.keySet().iterator(); iterator.hasNext(); ) {
				String key = iterator.next();

				boolean isfirst = true;
				StringBuffer tempData = new StringBuffer();
				List<String> columnKeys = mergecolums.get(key);
				for (Iterator<String> stringIterator = columnKeys.iterator(); stringIterator.hasNext(); ) {
					String columnKey = stringIterator.next();
					if(isfirst) {
						tempData.append(kv.get(columnKey));
						isfirst = false;
					} else {
						tempData.append(",").append(kv.get(columnKey));
					}
				}

				row.putData(key, tempData.toString());
			}
		}

		String ctype = ctypeSB.toString();
		if("insert".equals(type)) {
			row.setCtype(MurmurHash3.murmurhash3_x86_32(ctype, 0, ctype.length(), 2));
		} else if("delete".equals(type)) {
			row.setCtype(MurmurHash3.murmurhash3_x86_32(ctype, 0, ctype.length(), 3));
		} else {
			//this.writeOldData()处理
		}

	}

	private void writeOldData(Table table, RowMap row, Serializable[] oldData, BitSet oldIncludedColumns) {
		int dataIdx = 0, colIdx = 0;

		StringBuffer ctypeSB = new StringBuffer();

		for ( ColumnDef cd : table.getColumnList() ) {
			if ( oldIncludedColumns.get(colIdx) ) {
				Object json = null;
				if ( oldData[dataIdx] != null ) {
					json = cd.asJSON(oldData[dataIdx]);
				}

				ctypeSB.append(cd.getName()).append("_");

				if (!row.hasData(cd.getName())) {
					/*
					   If we find a column in the BEFORE image that's *not* present in the AFTER image,
					   we're running in binlog_row_image = MINIMAL.  In this case, the BEFORE image acts
					   as a sort of WHERE clause to update rows with the new values (present in the AFTER image),
					   In this case we should put what's in the "before" image into the "data" section, not the "old".
					 */
					row.putData(cd.getName(), json);
				}
//				else {
//					if (!Objects.equals(row.getData(cd.getName()), json)) {
//						row.putOldData(cd.getName(), json);
//					}
//				}
				row.putOldData(cd.getName(), json);
				dataIdx++;
			}
			colIdx++;
		}

		String ctype = ctypeSB.toString();
		row.setCtype(MurmurHash3.murmurhash3_x86_32(ctype, 0, ctype.length(), 1));
	}

	private RowMap buildRowMap(String type, Position position, Serializable[] data, Table table, BitSet includedColumns) {
		RowMap map = new RowMap(
			type,
			table.getDatabase(),
			table.getName(),
			event.getHeader().getTimestamp(),
			table.getPKList(),
			position
		);

		writeData(type, table, map, data, includedColumns);
		return map;
	}

	public List<RowMap> jsonMaps(Table table, Position lastHeartbeatPosition) {
		ArrayList<RowMap> list = new ArrayList<>();

		Position nextPosition = lastHeartbeatPosition.withBinlogPosition(this.nextPosition);
		switch ( getType() ) {
			case WRITE_ROWS:
			case EXT_WRITE_ROWS:
				for ( Serializable[] data : writeRowsData().getRows() ) {
					list.add(buildRowMap("insert", nextPosition, data, table, writeRowsData().getIncludedColumns()));
				}
				break;
			case DELETE_ROWS:
			case EXT_DELETE_ROWS:
				for ( Serializable[] data : deleteRowsData().getRows() ) {
					list.add(buildRowMap("delete", nextPosition, data, table, deleteRowsData().getIncludedColumns()));
				}
				break;
			case UPDATE_ROWS:
			case EXT_UPDATE_ROWS:
				for ( Map.Entry<Serializable[], Serializable[]> e : updateRowsData().getRows() ) {
					Serializable[] data = e.getValue();
					Serializable[] oldData = e.getKey();

					RowMap r = buildRowMap("update", nextPosition, data, table, updateRowsData().getIncludedColumns());
					writeOldData(table, r, oldData, updateRowsData().getIncludedColumnsBeforeUpdate());
					list.add(r);
				}
				break;
		}

		return list;
	}
}
