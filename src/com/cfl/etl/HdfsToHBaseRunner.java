package com.cfl.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 数据清洗：从hdfs中将ip转换为城市，存储到HBase
 * @author chenfenli
 *
 */
public class HdfsToHBaseRunner implements Tool{

	// hfds地址
	private static final String hdfsUrl = "hdfs://192.169.224.104:8020";
	// hdfs目录
	private static final String hdfsCatalog = "/flume/events/";
	// hbase中zookeeper地址
	private static final String zooKeeperUrl = "192.169.224.104";
	// 工作名称
	private static final String jobName = " hdfs_to_hbase";
	
	private Configuration conf = null;
		
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new HdfsToHBaseRunner(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Override
	public Configuration getConf() {
		return this.conf;
	}
	@Override
	public void setConf(Configuration conf) {
		conf.set("fs.defaultFS", hdfsUrl);
		conf.set("hbase.zookeeper.quorum", zooKeeperUrl);
		this.conf = HBaseConfiguration.create(conf);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configuration = this.conf;
		
		Job job = Job.getInstance(configuration,jobName);
		job.setJarByClass(HdfsToHBaseRunner.class);
		job.setMapperClass(HdfsToHBaseMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);
		
		// 设置输入路径
		FileSystem fs = null;
		try {
			fs = FileSystem.get(configuration);
			Path inputPath = new Path(hdfsCatalog + "20190622" + "/");
			if(fs.exists(inputPath)) {
				FileInputFormat.addInputPath(job, inputPath);
			} else {
				throw new RuntimeException("文件不存在：" + inputPath);
			}
		} catch (Exception e) {
			throw new RuntimeException("设置job的mapreduce输入路径出现异常", e);
		}
		
		// 集群上运行，打成jar运行(要求addDependencyJars参数为true，默认就是true)。
		// 本地运行，要求参数addDependencyJars为false
		TableMapReduceUtil.initTableReducerJob("hadoop_log", null, job, null, null,null, null, false);
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
}
