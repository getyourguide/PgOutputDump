package org.getyourguide.source;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class FileStreamSource implements Source {

    private int count = 0;
    private final DataInputStream dataInputStream;

    public FileStreamSource(String fileName) {
        try {
            dataInputStream = new DataInputStream(new FileInputStream(fileName));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + fileName);
        }
    }

    @Override
    public ByteBuffer readPending() throws Exception {
        count++;
        if (dataInputStream.available() == 0) {
            System.out.printf("End of File: %s events read\n", count);
            throw new RuntimeException("End of File");
        }

        // Read the size of the message
        var size = dataInputStream.readInt();
        System.out.printf("Reading %s bytes\n", size);

        // Read the message
        var data = dataInputStream.readNBytes(size);
        return ByteBuffer.wrap(data);
    }

    @Override
    public long getLastReceiveLSN() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        dataInputStream.close();
    }
}
