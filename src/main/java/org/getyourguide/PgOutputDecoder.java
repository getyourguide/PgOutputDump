package org.getyourguide;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.getyourguide.models.Column;
import org.getyourguide.models.Table;
import org.getyourguide.utils.PgOutputUtil;

public class PgOutputDecoder {

    private static final LocalDateTime PG_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
    Map<Integer, Table> mapOfTables = new HashMap<>();
    private String previousType = "";

    // https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html
    public void decodeMessage(ByteBuffer msg) {
        var byteOne = msg.get();

        var type = new String(new byte[] {byteOne}, 0, 1);

        switch (type) {
            case "B":
                {
                    var lsn = msg.getLong();
                    var ts = msg.getLong();
                    var xid = msg.getInt();

                    System.out.printf(
                            "Begin LSN %s, timestamp %s, xid %s - %s%n",
                            lsn,
                            PG_EPOCH.plusNanos(ts * 1000L), // ts is microseconds since EPOCH
                            xid,
                            ts);
                    break;
                }
            case "R":
                {
                    var id = msg.getInt();
                    var namespace = PgOutputUtil.readString(msg);
                    var relation = PgOutputUtil.readString(msg);
                    var replicaIdentity = msg.get();
                    var nColumns = msg.getShort();

                    var table = new Table(namespace, relation);

                    // Read columns information

                    List<Column> columnNames = new ArrayList<>();

                    for (int i = 0; i < nColumns; i++) {
                        var isKey = msg.get();
                        var columnName = PgOutputUtil.readString(msg);
                        var columnTypeId = msg.getInt();
                        var atttypmod = msg.getInt();
                        columnNames.add(new Column(i, isKey == 1, columnName, columnTypeId));
                    }

                    if (!mapOfTables.containsKey(id)) {
                        mapOfTables.put(id, table);
                    }

                    table.setColumns(columnNames);

                    System.out.printf(
                            "Table %s with %s columns: %s%n", relation, nColumns, columnNames);
                    break;
                }
            case "C":
                {
                    msg.get();
                    var lsn = msg.getLong();
                    var endLsn = msg.getLong();
                    var ts = msg.getLong();
                    System.out.printf(
                            "Commit: LSN %s, end LSN %s, ts %s%n",
                            lsn, endLsn, PG_EPOCH.plusNanos(ts * 1000L));
                    break;
                }
            case "D":
                {
                    var relationId = msg.getInt();

                    System.out.printf("Delete from %s%n", mapOfTables.get(relationId).getName());

                    var byte1 = msg.get();
                    var tupleType = new String(new byte[] {byte1}, 0, 1);

                    // TupleData
                    PgOutputUtil.readTupleData(msg);
                    break;
                }
            case "I":
                {
                    var id = msg.getInt();

                    System.out.printf(
                            "Insert into %s (%s) %n",
                            mapOfTables.get(id).getName(),
                            mapOfTables.get(id).getColumns().stream()
                                    .map(Column::getName)
                                    .collect(Collectors.joining(", ")));

                    var byte1 = msg.get();
                    var newTuple = new String(new byte[] {byte1}, 0, 1);

                    assert newTuple.equals("N");

                    PgOutputUtil.readTupleData(msg);
                    break;
                }
            case "U":
                {
                    var relationId = msg.getInt();

                    System.out.printf("Update table %s%n", mapOfTables.get(relationId).getName());

                    var byte1 = msg.get();
                    var newTuple = new String(new byte[] {byte1}, 0, 1);

                    switch (newTuple) {
                        case "K":
                            System.out.println("Key update");
                            System.out.println("Old Key");
                            PgOutputUtil.readTupleData(msg);
                            break;
                        case "O":
                            System.out.println("Value update");
                            System.out.println("Old Value");
                            PgOutputUtil.readTupleData(msg);
                            break;
                        case "N":
                            System.out.println("New value");
                            PgOutputUtil.readTupleData(msg);
                            return;
                        default:
                            System.out.printf("K or O Byte1 not set, got instead %s%n", newTuple);
                    }

                    byte1 = msg.get();
                    newTuple = new String(new byte[] {byte1}, 0, 1);

                    assert newTuple.equals("N");

                    System.out.println("New value");
                    PgOutputUtil.readTupleData(msg);

                    break;
                }
            case "M":
                {
                    if (previousType.equals("S")) {
                        msg.getInt(); // TransactionId
                    }
                    var trx = msg.get(); // Transactional flag
                    var lsn = msg.getLong(); // The LSN of the logical decoding message.

                    var prefix = PgOutputUtil.readString(msg);
                    var size = msg.getInt();
                    // we assume payload is a string
                    StringBuilder sb = new StringBuilder();
                    byte b;
                    for (int i = 0; i < size; i++) {
                        b = msg.get();
                        sb.append((char) b);
                    }
                    var message = sb.toString();
                    System.out.printf(
                            "[LSN: %s] [Trx: %s] Prefix: %s, Message (size: %s): %s\n",
                            lsn, trx, prefix, size, message);
                    break;
                }
            default:
                {
                    System.out.printf("Type %s not implemented", type);
                }
                previousType = type;
        }
    }
}
