package com.sathish.bigdata.empdetail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EmpSalaryAnalyser {

	public static void main(String[] args) {
		Configuration conf = new Configuration();		
		try {
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.err.println("Usage: wordcount <in> <out>");
				System.exit(2);
			}
			Job job = new Job(conf,"Emp Salary Details");
			job.setJarByClass(EmpSalaryAnalyser.class);
			job.setMapperClass(EmpSalaryMapper.class);
			job.setReducerClass(EmpSalaryReducer.class);
			job.setCombinerClass(EmpSalaryReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			System.err.println("Exception in executing EMpDetails job : "+e.getMessage());
		}
	}

}
