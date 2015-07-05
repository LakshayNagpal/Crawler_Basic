
import java.io.IOException;
/*import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;*/
import java.util.StringTokenizer;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public class mcrawler
extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text url = new Text();
	private Text word = new Text();
    
@Override
public void map(LongWritable Key, Text value, Context context)
throws IOException, InterruptedException{
	
	String abc = value.toString();
	StringTokenizer itr = new StringTokenizer(abc);
	int i=0;
	  while(itr.hasMoreTokens()){
		  
		 if(i==0) url.set(itr.nextToken());
		  if(i==1) word.set(itr.nextToken());
		  ++i;
	  }
	  
		  context.write(url,word); 
		  
		}


}
