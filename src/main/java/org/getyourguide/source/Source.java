package org.getyourguide.source;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface Source extends Closeable {
    ByteBuffer readPending() throws Exception;

    long getLastReceiveLSN();
}
