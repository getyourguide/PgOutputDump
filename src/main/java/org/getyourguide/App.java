package org.getyourguide;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.getyourguide.source.FileStreamSource;
import org.getyourguide.source.PgReplicationSource;
import org.getyourguide.source.Source;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class App implements Callable<Integer> {

    private static final Logger LOGGER = Logger.getLogger(App.class.getName());

    @Option(names = "--ssl", description = "Use secured connection")
    boolean useSsl;

    @Option(names = "--silent", description = "Do not display messages")
    boolean isSilent;

    @Option(names = "--dump", description = "File name to dump to")
    String outFileName;

    @Option(
            names = {"-h", "--host"},
            description = "The database hostname",
            required = false)
    private String host;

    @Option(
            names = {"-f", "--file"},
            description = "The WAL archive file to read from",
            required = false)
    private String file;

    @Option(
            names = {"-d", "--database"},
            description = "The database to connect to",
            required = false)
    private String dbName;

    @Option(
            names = {"-U", "--user"},
            description = "The user login",
            required = false)
    private String userName;

    @Option(
            names = {"-P", "--password"},
            description = "The user password",
            required = false)
    private String password;

    @Option(
            names = {"-s", "--slot"},
            description = "The name of the slot, must already exist",
            required = false)
    private String slot;

    @Option(
            names = {"-p", "--publication"},
            description = "The name of the publication, must already exist",
            required = false)
    private String publication;

    @Option(
            names = {"-c", "--count"},
            description = "Exit application after reading this number of messages",
            required = false)
    private Integer maxCount;

    private Connection conn;
    private DataOutputStream dos;
    private Source source;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws SQLException, InterruptedException, IOException {
        LOGGER.info("Application starting");

        if (file == null) {
            String url = String.format("jdbc:postgresql://%s/%s?ssl=%s", host, dbName, useSsl);
            Properties props = new Properties();
            PGProperty.USER.set(props, userName);
            PGProperty.PASSWORD.set(props, password);
            PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
            PGProperty.REPLICATION.set(props, "database");
            PGProperty.PREFER_QUERY_MODE.set(props, "simple");

            conn = DriverManager.getConnection(url, props);
            PGConnection replConnection = conn.unwrap(PGConnection.class);
            source = new PgReplicationSource(replConnection, slot, publication);
        } else {
            source = new FileStreamSource(file);
        }

        if (outFileName != null) {
            File file = new File(outFileName);
            var fos = new FileOutputStream(file);
            this.dos = new DataOutputStream(fos);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::close) {});

        var decoder = new PgOutputDecoder();

        var msgCount = 0;
        var byteWritten = 0;

        while (!closed.get()) {

            ByteBuffer msg;

            try {
                msg = source.readPending();
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }

            var lsn = source.getLastReceiveLSN();

            if (msg == null) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }

            msgCount += 1;

            if (outFileName != null) {
                int len = msg.remaining();
                byte[] data = new byte[len];
                msg.get(data);

                for (int i = 0; i < len + 4; i++) {
                    byteWritten++;
                    if (byteWritten % (10 * 1024 * 1024) == 0) {
                        System.out.printf(
                                "[%s] %s MB written so far to file\n",
                                LocalDateTime.now(), dos.size() / 1024 / 1024);
                    }
                }

                dos.writeInt(len);
                dos.write(data);
                msg.rewind();
            }

            if (isSilent) continue;

            System.out.printf("----| %s |----\n", lsn);
            decoder.decodeMessage(msg);

            if (maxCount != null && msgCount == maxCount) {
                break;
            }
        }
        close();
        return 0;
    }

    private void close() {
        System.out.print("Closing application\n");
        closed.set(true);
        try {
            dos.flush();
            dos.close();
            source.close();
            conn.close();
        } catch (Exception ignore) {
            // no handling
        }
    }
}
