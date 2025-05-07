// package com.hadoop.lesson.sort;

// import java.io.IOException;

// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.NullWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;

// public class SortMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
//     private IntWritable num = new IntWritable();

//     @Override
//     protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//         try {
//             int number = Integer.parseInt(value.toString());
//             num.set(number);
//             context.write(NullWritable.get(), num);
//         } catch (NumberFormatException e) {
//             System.err.println("Invalid input: " + value.toString());
//         }
//     }
// }