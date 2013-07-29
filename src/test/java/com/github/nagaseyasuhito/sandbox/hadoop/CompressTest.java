package com.github.nagaseyasuhito.sandbox.hadoop;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;

public class CompressTest {
	private void write(String path, Configuration conf, CompressionCodec codec) throws Throwable {
		conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, 1);

		RCFile.Writer writer = new RCFile.Writer(FileSystem.get(conf), conf, new Path("file://" + path), null, codec);

		BytesRefArrayWritable writable = new BytesRefArrayWritable();
		writable.set(0, new BytesRefWritable(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));
		writer.append(writable);
		writer.close();
	}

	private void read(String path, Configuration conf) throws Throwable {
		RCFile.Reader reader = new RCFile.Reader(FileSystem.get(conf), new Path("file://" + path), conf);

		while (reader.next(new LongWritable())) {
			reader.getCurrentRow(new BytesRefArrayWritable());
		}
	}

	@Test
	public void withoutCodec() throws Throwable {
		File path = File.createTempFile("hadoop", ".plain");
		Configuration conf = new Configuration();

		this.write(path.getPath(), conf, null);
		this.read(path.getPath(), conf);
	}

	@Test
	public void withBuiltinGzipCodec() throws Throwable {
		File path = File.createTempFile("hadoop", ".gz");
		Configuration conf = new Configuration();
		conf.setBoolean(CommonConfigurationKeysPublic.IO_NATIVE_LIB_AVAILABLE_KEY, false);

		this.write(path.getPath(), conf, new GzipCodec());
		this.read(path.getPath(), conf);
	}

	@Test
	public void withGzipCodec() throws Throwable {
		File path = File.createTempFile("hadoop", ".gz");
		Configuration conf = new Configuration();

		this.write(path.getPath(), conf, new GzipCodec());
		this.read(path.getPath(), conf);
	}
}
