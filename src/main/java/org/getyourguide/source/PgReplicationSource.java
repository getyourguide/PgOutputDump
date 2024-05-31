package org.getyourguide.source;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationStream;

public class PgReplicationSource implements Source {

    private final PGConnection replConnection;
    private PGReplicationStream stream;

    public PgReplicationSource(PGConnection replConnection, String slot, String publication) {
        this.replConnection = replConnection;

        try {
            stream =
                    replConnection
                            .getReplicationAPI()
                            .replicationStream()
                            .logical()
                            .withSlotName(slot)
                            .withSlotOption("proto_version", 1)
                            .withSlotOption("publication_names", publication)
                            .start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start replication stream", e);
        }
    }

    @Override
    public ByteBuffer readPending() throws Exception {
        return stream.readPending();
    }

    @Override
    public long getLastReceiveLSN() {
        return stream.getLastReceiveLSN().asLong();
    }

    @Override
    public void close() throws IOException {
        try {
            stream.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
