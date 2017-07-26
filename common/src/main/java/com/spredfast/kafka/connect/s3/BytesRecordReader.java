package com.spredfast.kafka.connect.s3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.spredfast.kafka.connect.s3.RecordReader;

/**
 * Helper for reading raw length encoded records from a chunk file. Not thread safe.
 */
public class BytesRecordReader implements RecordReader {
	private static final Logger log = LoggerFactory.getLogger(BytesRecordReader.class);

	private final ByteBuffer lenBuffer = ByteBuffer.allocate(4);

	private final boolean includesKeys;

	/**
	 * @param includesKeys do the serialized records include keys? Or just values?
	 */
	public BytesRecordReader(boolean includesKeys) {
		this.includesKeys = includesKeys;
	}

	/**
	 * Reads a record from the given uncompressed data stream.
	 *
	 * @return a raw ConsumerRecord or null if at the end of the data stream.
	 */
	@Override
	public ConsumerRecord<byte[], byte[]> read(String topic, int partition, long offset, BufferedInputStream data) throws IOException {
		final byte[] key;
		final Integer valSize;
		if (includesKeys) {
			final Integer keySize = readLen(topic, partition, offset, data);
			if (keySize == null) {
				log.warn(String.format("Failed to calculate key size, skipping. Topic: %s, Partition: %d, Offset: %d", topic, partition, offset));
				return null;
			}
			key = readBytes(keySize, data, topic, partition, offset);
			if(key == null) {
				log.warn(String.format("Key not found, skipping. Topic: %s, Partition: %d, Offset: %d", topic, partition, offset));
				return null;
			}

			valSize = readLen(topic, partition, offset, data);
			if(valSize == null) {
				log.warn(String.format("Failed to calculate value size, skipping. Topic: %s, Partition: %d, Offset: %d", topic, partition, offset));
				return null;
			}
		} else {
			key = null;
			Integer vSize = readLen(topic, partition, offset, data);
			if (vSize == null) {
				log.warn(String.format("Failed to calculate value size, skipping. Topic: %s, Partition: %d, Offset: %d", topic, partition, offset));
				return null;
			}
			valSize = vSize;
		}

		final byte[] value = readBytes(valSize, data, topic, partition, offset);

		return new ConsumerRecord<>(topic, partition, offset, key, value);
	}

	private byte[] readBytes(int size, InputStream data, String topic, int partition, long offset) throws IOException {
		final byte[] bytes = new byte[size];
		int read = 0;
		while (read < size) {
			final int readNow = data.read(bytes, read, size - read);
			if (readNow == -1) {
				return null;
			}
			read += readNow;
		}
		return bytes;
	}

	private Integer readLen(String topic, int partition, long offset, InputStream data) throws IOException {
		lenBuffer.rewind();
		int read = data.read(lenBuffer.array(), 0, 4);
		if (read == -1) {
			return null;
		} else if (read != 4) {
			return null;
		}
		return lenBuffer.getInt();
	}
}
