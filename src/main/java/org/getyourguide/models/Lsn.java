package org.getyourguide.models;

import java.nio.ByteBuffer;

public class Lsn {
    private final long value;

    public Lsn(long lsn) {
        this.value = lsn;
    }

    @Override
    public String toString() {
        return String.format("%s, [hex:%s]", value, asHexString());
    }

    private String asHexString() {
        final ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(value);
        buf.position(0);

        final int logicalXlog = buf.getInt();
        final int segment = buf.getInt();
        return String.format("%X/%X", logicalXlog, segment);
    }
}
