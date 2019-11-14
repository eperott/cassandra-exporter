package com.zegelin.prometheus.exposition;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

interface ExpositionSink<T> {
    void writeByte(int asciiChar);

    void writeBytes(ByteBuffer nioBuffer);

    void writeAscii(String asciiString);

    void writeUtf8(String utf8String);

    void writeFloat(float value);

    T getBuffer();

    int getIngestedByteCount();
}
