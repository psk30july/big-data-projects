package com.sathish.bigdata.empdetail;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpSalaryReducer extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
	
		int maxSalary = 0;
		Text outputText = null;
		for (Text line : value) {
			String emp = line.toString();
			String[] empValue = emp.split("\t");
			int salary = Integer.parseInt(empValue[5].trim());
	        if(salary > maxSalary){
	        	maxSalary = salary;
	        	outputText = line;
	        }
		}
		context.write(key, outputText);
	}

}
