package com.example.kafkalogging.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HeaderUtil {

    public static Map<String, Serializable> convertToSerializableMap(Headers headers) {
        return Arrays.stream(headers.toArray()).collect(
                Collectors.toMap(Header::key, HeaderUtil::convertToSerializable, (existing, replacement) -> existing + "," + replacement));
    }

    public static Serializable convertToSerializable(Header header) {
        byte[] value = header.value();
        if (value == null) return "null";
        if (value.length == Integer.BYTES) {
            try {
                return ByteBuffer.wrap(value).getInt();
            } catch (Exception e) {
                // ignore
            }
        }
        if (value.length == Long.BYTES) {
            try {
                return ByteBuffer.wrap(value).getLong();
            } catch (Exception e) {
                // ignore
            }
        }
        if (value.length == 6) {
            try {
                ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
                byteBuffer.put(new byte[]{0, 0});
                byteBuffer.put(value);
                byteBuffer.flip();
                return byteBuffer.getLong();
            } catch (Exception e) {
                // ignore
            }
        }
        return new String(value);
    }
}
