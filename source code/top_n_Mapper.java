package top_n_records;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Map.Entry;

public class top_n_Mapper extends Mapper<Object,Text,Text,LongWritable> {

	public SortedMap<Long,String> tmap;
	genericDriver g;
	public int trial;
	@Override
	public void setup(Context context)throws IOException,InterruptedException
	{
		tmap = new TreeMap<Long,String>();
		g= new genericDriver();
		trial=0;
	}
	
	
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{	
		     //input data format => movie_name    no_of_views  (tab seperated)
		
		
				//we split the input data 
				String []tokens = value.toString().split("\t");
				
				String movie_name = tokens[0];
				long no_of_views = Long.parseLong(tokens[1]);
				
				
				//insert data into treeMap , we want top 10  viewed movies
				//so we pass no_of_views as key
				tmap.put(no_of_views, movie_name);   
			   
				
				//we remove the first key-value if it's size increases 10 
				
				//int x=g.getNum();
				
				
				if(tmap.size()>5)
				{
					tmap.remove(tmap.lastKey());
					//g.reduceNum();
				}
				/*if(x>5)
				{
					tmap.remove(tmap.lastKey());
					g.reduceNum();
				}*/
		
		
	}
	
	@Override
	public void cleanup(Context context)throws IOException,InterruptedException
	{
		
		 SortedMap<Long,String> test = new TreeMap<Long,String>();
		 
		 
		 while(tmap.size()>5)
			{
				tmap.remove(tmap.lastKey());
			}
		 
		 top_n_Reducer map = new top_n_Reducer();

		for(Map.Entry<Long,String> entry : tmap.entrySet()) {
			 if((trial<5)&&(map.trial2<5)) {
			 long count = entry.getKey();         
			  String name = entry.getValue();
			  
			 
			  test.put(count, name);
			  if((test.size()>5)&&(test.size()>5))
				{
				  tmap.remove(test.lastKey());
				  test.remove(test.lastKey());
				}else {
				
					int x=g.getNum();
					if(x<=5) {
						g.updateNum();
					 context.write(new Text(name),new LongWritable(count));
					 
					}
				
				}
			  trial=trial+1;
			 }
			}
		
	}
	
}