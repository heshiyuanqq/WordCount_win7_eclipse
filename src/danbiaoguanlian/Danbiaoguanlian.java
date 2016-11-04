package danbiaoguanlian;

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
import java.util.ArrayList;
import java.util.Iterator;
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
//5.求平均值√
//6.单表关联(给出child-parent（孩子——父母）表，要求输出grandchild-grandparent（孙子——爷奶）表。)
//7.多表关联
//8.倒排索引
public class Danbiaoguanlian {
 
		//前两个类型是map的参数类型，后两个类型是context.write的参数类型
	  public static class MyMapper  extends Mapper<Object, Text, Text, Text>{
		  		//参数value表示一行?这样虽然是排序但是去重了
		  		public static IntWritable int_one=new IntWritable(1);
			    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			    		StringTokenizer tokenizer = new StringTokenizer(value.toString());
			    		String col_children=tokenizer.nextToken();
			    		String col_parent=tokenizer.nextToken();
			    		/**
			    		 * 要连接的是左表的parent列和右表的child列，且左表和右表是同一个表，
			    		 * 所以在map阶段将读入数据分割成child和parent之后，会将parent设置成key，
			    		 * child设置成value进行输出，并作为左表；再将同一对child和parent中的child设置
			    		 * key，parent设置成value进行输出，作为右表。为了区分输出中的左右表，
			    		 * 需要在输出的value中再加上左右表的信息，比如在value的String最开始处加上字符1表示左表，
			    		 * 加上字符2表示右表。
			    		 */
			    		//要连接的是左表的parent列和右表的child列
			    		//要连接的必须作为key
			    		 //value第一个字符为0：表示左表，为1：表示右表
			    		//左表
			    		context.write(new Text(col_parent), new Text(0+col_children));
			    		//右表
			    		context.write(new Text(col_children), new Text(1+col_parent));
			    }
	  }
	  
	  
	  public static class MyCombiner  extends Reducer<Text,Text,Text,Text> {
	  		@Override
	  		protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	  				 for (Text text : values) {
	  					 	context.write(key, text);
	  				 }
		  	}
}
	  //map ---> combiner --->reduce(注意：Output types of a combiner must == output types of a mapper. )
	  	
	  //前两个类型是reduce的前两个参数类型,后两个类型是context.write的参数类型
	  public static class MyReducer  extends Reducer<Text,Text,Text,Text> {
		  		public static Text emptyText=new Text();
		  		@Override
		  		protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		  			/*context.write(new Text(col_parent), new Text(0+col_children));
		    		context.write(new Text(col_children), new Text(1+col_parent));*/
		  			ArrayList<String> list_pre0 = new ArrayList<String>();
		  			ArrayList<String> list_pre1 = new ArrayList<String>();
		  			
	  				  for (Text text : values) {
	  					   String str=text.toString();
	  					   if(str.startsWith("0")){
	  						   list_pre0.add(str.substring(1));
	  					   }else{
	  						   list_pre1.add(str.substring(1));
	  					   }
					  }
	  				  
	  				  
	  				  if(list_pre0.size()==0||list_pre1.size()==0){
	  					  return;
	  				  }
	  				  
	  				  for(String child:list_pre0){
	  					  for(String grandpa:list_pre1){
	  						  	context.write(new Text(child), new Text(grandpa));
	  					  }
	  				  }
			  	}
	  }
	 
	  public static void main(String[] xx)  {
		  		boolean isTest=false;
		  		if(isTest){
		  			//测试代码
		  			StringTokenizer tokenizer = new StringTokenizer("张三 46");
		  			String name=tokenizer.nextToken();
		    		double score=Double.parseDouble(tokenizer.nextToken());
		  			System.out.println("name="+name+",score="+score);
		  			return;
		  		}
		  
		  
			  try{
				    Job job = new Job(new Configuration(), "win7中eclipse中word count_4(测试检测海量文章中每个单词的数量耗时！)");
				    
				    job.setJarByClass(Danbiaoguanlian.class);
				    
				    job.setMapperClass(MyMapper.class);
				    job.setCombinerClass(MyCombiner.class);
				    job.setReducerClass(MyReducer.class);
				    
				    job.setMapOutputKeyClass(Text.class);
				    job.setMapOutputValueClass(Text.class);
				    
				    job.setOutputKeyClass(Text.class);
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