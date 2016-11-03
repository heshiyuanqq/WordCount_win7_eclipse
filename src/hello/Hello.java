package hello;
 
import java.io.IOException;
import java.io.StringWriter;
import java.util.StringTokenizer;
 






import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 /**
  * Type mismatch in value from map: 
  * expected org.apache.hadoop.io.IntWritable, 
  * received org.apache.hadoop.io.Text

  * @author Administrator
  *
  */

//1.统计单词个数(某某单词：个数)
//2.统计单词个数(某某单词：....几个就几个点儿)
public class Hello {
 
		//前两个类型是map的参数类型，后两个类型是context.write的参数类型
	  public static class TokenizerMapper  extends Mapper<Object, Text, Text, IntWritable>{
			    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				      StringTokenizer itr = new StringTokenizer(value.toString());
				      Text text1 = new Text();
				      while (itr.hasMoreTokens()) {
					        text1.set(itr.nextToken());
					        context.write(text1, new IntWritable(1));
					        //这里写出去的每个市这样的：xxx:1
				      }
			    }
			
			  
	  }
	  
	  /**
	   *xxx:1      
				==>	xxx:<1,1> 
	   *xxx:1
	   *
	   *
	   *
	   *xxx:1
	   *xxx:1	==> xxx:<1,1,1>   ===>    xxx:<<1,1>,<1,1,1>,<1,1>>
	   *xxx:1
	   *
	   *
	   *
	   *xxx:1
	   *		==>	xxx:<1,1>
	   *xxx:1
	   *
	   */
 
	  //前两个类型是reduce的前两个参数类型,后两个类型是context.write的参数类型
	  public static class IntSumReducer  extends Reducer<Text,IntWritable,Text,IntWritable> {
		  		@Override
		  		protected void reduce(Text key, Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		  			//这里接收的key:values
		  			/**
		  			 * xxx:<>
		  			 */
		  				  IntWritable intWritable = new IntWritable(0);
		  				  int sum=0;
					      for (IntWritable val : values) {
					    	  	intWritable.set(intWritable.get()+val.get());
					      }
					      context.write(key, intWritable);//会出现这样的问题"4	***(星级)*(星级)(星级)",知道为什么吗？因为是分布分工合作的，可能同样的key在很多机器的很多xiancheng进程任务中都有，最后被写到了一起
			  	}
	  }
	 
	  public static void main(String[] xx)  {
			  try{
				    System.setProperty("hadoop.home.dir", "D:\\install\\hadoop-2.6.0\\hadoop-2.6.0");
				    Job job = new Job(new Configuration(), "win7中eclipse中word count_4(测试检测海量文章中每个单词的数量耗时！)");
				    
				    job.setJarByClass(Hello.class);
				    
				    job.setMapperClass(TokenizerMapper.class);
				    
				    job.setCombinerClass(IntSumReducer.class);
				    job.setReducerClass(IntSumReducer.class);
				    
				    
				    job.setOutputKeyClass(Text.class);//reduce第一个参数类型(要和map中的context.write的第一个参数类型一致)
				    job.setOutputValueClass(IntWritable.class);//reduce第二个参数类型(要和map中的context.write的第二个参数类型一致)
				    
				    
				    
				 /* 这样设置：  job.setOutputKeyClass(Text.class);
				    job.setOutputValueClass(IntWritable.class);
				    
				    报错：Type mismatch in value from map: 
					  * expected org.apache.hadoop.io.IntWritable, 
					  * received org.apache.hadoop.io.Text
				    */
				    
				    
				    
				    /**不设置：
				     *报错：Type mismatch in key from map:
				     *   expected org.apache.hadoop.io.LongWritable, 
				     *   received org.apache.hadoop.io.Text

				     */
				    /**
				     * 
				     */
				    
				  
				    FileInputFormat.addInputPath(job, new Path("input"));
				    FileOutputFormat.setOutputPath(job, new Path("output"));
				    
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }catch(Exception e){
				    e.printStackTrace();
			  }
	  }
}