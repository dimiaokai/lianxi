package com.cfl.etl;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cfl.util.IPSeekerExt;
import com.cfl.util.IPSeekerExt.RegionInfo;
import com.cfl.util.TimeUtil;

public class HdfsToHBaseMapper extends Mapper<LongWritable, Text, NullWritable, Put>{

	private static String family = "log";
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Put>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		if(StringUtils.isNotEmpty(str)) {
			// 解析日志信息，并获取城市
			String[] ss = str.split("\\^A");
			String ip = ss[0];
			String date = String.valueOf(TimeUtil.parseNginxServerTime2Long(ss[1].trim()));
			String url = ss[3];
			RegionInfo regionInfo = new IPSeekerExt().analyticIp(ip);
			String city = regionInfo.getCity();
			
			System.out.println("日志：" + str);
			
			// 插入到hdfs
			Put put = new Put(Bytes.toBytes(date));
			put.add(Bytes.toBytes(family), Bytes.toBytes("ip"), Bytes.toBytes(ip));
			put.add(Bytes.toBytes(family), Bytes.toBytes("city"), Bytes.toBytes(city));
			put.add(Bytes.toBytes(family), Bytes.toBytes("date"), Bytes.toBytes(date));
			put.add(Bytes.toBytes(family), Bytes.toBytes("url"), Bytes.toBytes(url));
			context.write(NullWritable.get(), put);
		}
	}
}
