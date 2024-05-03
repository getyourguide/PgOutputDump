package org.getyourguide;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class App implements Callable<Integer> {

    @Option(names = "--ssl", description = "Use secured connection")
    boolean useSsl;

    @Option(
            names = {"-h", "--host"},
            description = "The database hostname",
            required = true)
    private String host;

    @Option(
            names = {"-d", "--database"},
            description = "The database to connect to",
            required = true)
    private String dbName;

    @Option(
            names = {"-U", "--user"},
            description = "The user login",
            required = true)
    private String userName;

    @Option(
            names = {"-P", "--password"},
            description = "The user password",
            required = true)
    private String password;

    @Option(
            names = {"-s", "--slot"},
            description = "The name of the slot, must already exist",
            required = true)
    private String slot;

    @Option(
            names = {"-p", "--publication"},
            description = "The name of the publication, must already exist",
            required = true)
    private String publication;

    @Option(
            names = {"-c", "--count"},
            description = "Exit application after reading this number of messages",
            required = false)
    private Integer maxCount;

    private PGReplicationStream stream;
    private Connection conn;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws SQLException, InterruptedException {

        String url = String.format("jdbc:postgresql://%s/%s?ssl=%s", host, dbName, useSsl);
        Properties props = new Properties();
        PGProperty.USER.set(props, userName);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        conn = DriverManager.getConnection(url, props);
        PGConnection replConnection = conn.unwrap(PGConnection.class);

        stream =
                replConnection
                        .getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(slot)
                        .withSlotOption("proto_version", 4)
                        .withSlotOption("publication_names", publication)
                        .withSlotOption("messages", "true")
                        .start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::close) {});

        var decoder = new PgOutputDecoder();

        var msgCount = 0;

        while (true) {

            ByteBuffer msg = stream.readPending();

            if (msg == null) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }

            msgCount += 1;

            decoder.decodeMessage(msg);

            if (maxCount != null && msgCount == maxCount) {
                break;
            }
        }
        close();
        return 0;
    }

    private void close() {
        try {
            stream.close();
            conn.close();
        } catch (Exception ignore) {
            // no handling
        }
    }
}
