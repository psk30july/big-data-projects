package com.sathish.weblog.usercount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String fullLine = value.toString();
		String[] splitValues = fullLine.split("\t");
		String ipaddress = null;
		if(splitValues.length > 0){
			ipaddress = splitValues[0];
		}
		context.write(new Text(ipaddress), new IntWritable(1));
	}

}
