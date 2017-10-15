package com.sathish.bigdata.empdetail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpSalaryMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineValue = line.split("\t");
		String dept = lineValue[4].trim();
		String gender = lineValue[3].trim();
        context.write(new Text(dept+"_"+gender), value);
	}
}
