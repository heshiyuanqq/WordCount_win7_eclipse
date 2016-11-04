package otherDemo;

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

//1.统计单词个数(某某单词：个数)√
//2.统计单词个数(某某单词：....几个就几个点儿)√
//3.数据去重√
//4.成绩排名(字典排序√，大数字小排序)
//5.求平均值
//6.单表关联
//7.多表关联
//8.倒排索引
public class SortByNumber {
 
		//前两个类型是map的参数类型，后两个类型是context.write的参数类型
	  public static class MyMapper  extends Mapper<Object, Text, IntWritable, IntWritable>{
		  		//参数value表示一行?这样虽然是排序但是去重了
		  		public static IntWritable int_one=new IntWritable(1);
			    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				     	context.write(new IntWritable(Integer.parseInt(value.toString())), int_one);
			    }
	  }
	  
	  
	  public static class MyCombiner  extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	  		@Override
	  		protected void reduce(IntWritable key, Iterable<IntWritable> values,Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
	  				  int count=0;
	  				  for (IntWritable intWritable : values) {
	  					  count+=intWritable.get();
					  }
	  				  context.write(key, new IntWritable(count));
		  	}
}
	  //map ---> combiner --->reduce(注意：Output types of a combiner must == output types of a mapper. )
	  	
	  //前两个类型是reduce的前两个参数类型,后两个类型是context.write的参数类型
	  public static class MyReducer  extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
		  		public static Text emptyText=new Text();
		  		@Override
		  		protected void reduce(IntWritable key, Iterable<IntWritable> values,Reducer<IntWritable, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		  				  int count=0;
		  				  for (IntWritable intWritable : values) {
		  					  count+=intWritable.get();
						  }
		  				  for(int i=0;i<count;i++){
		  					  context.write(key, emptyText);
		  				  }
			  	}
	  }
	 
	  public static void main(String[] xx)  {
			  try{
				    Job job = new Job(new Configuration(), "win7中eclipse中word count_4(测试检测海量文章中每个单词的数量耗时！)");
				    
				    job.setJarByClass(SortByNumber.class);
				    
				    job.setMapperClass(MyMapper.class);
				    job.setCombinerClass(MyCombiner.class);
				    job.setReducerClass(MyReducer.class);
				    
				    job.setMapOutputKeyClass(IntWritable.class);
				    job.setMapOutputValueClass(IntWritable.class);
				    
				    job.setOutputKeyClass(IntWritable.class);
				    job.setOutputValueClass(Text.class);
				    
				    /*注意combiner的输入和输出要和map一致，所以job.setMapOutputValueClass就相当于设置了map和combiner，
				  	 * 而job.setOutputValueClass是设置reduce的输出(如果没有上面的话相当于设置了map,combiner,reduce他们三个)*/
				    FileInputFormat.addInputPath(job, new Path("input"));
				    FileOutputFormat.setOutputPath(job, new Path("output"));
				    
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }catch(Exception e){
				    e.printStackTrace();
			  }
	  }
}