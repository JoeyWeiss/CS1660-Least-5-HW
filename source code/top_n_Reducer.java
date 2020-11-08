package top_n_records;

import java.io.IOException; 	
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap; 

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class top_n_Reducer extends Reducer<Text, LongWritable, LongWritable, Text> { 
	
	public TreeMap<Long,String> tmap2;
	genericDriver g;
	public int trial2;
	@Override
	public void setup(Context context)throws IOException,InterruptedException
	{
		tmap2 = new TreeMap<Long,String>();
		g = new genericDriver();
		trial2=0;
	}
	
	
	@Override
	public void reduce(Text key,Iterable<LongWritable>values,Context context)throws IOException,InterruptedException
	{	
		     //input data from mapper 
			//key                values
			//movie_name         [ count ]
		    
		String name = key.toString();
		long count = 0;
		
		for(LongWritable val:values)
		{
			 count =count+ val.get();
		}
				
				
		   //insert data into treeMap , we want top 10  viewed movies
			//so we pass count as key
			
			tmap2.put(count, name);   
			   
				
				//we remove the first key-value if it's size increases 10 
				
				
				//int x=g.getNum();
				
				if(tmap2.size()>5)
				{
					tmap2.remove(tmap2.lastKey());
				
				}
				/*
				if(x>5)
				{
					tmap2.remove(tmap2.lastKey());
				}
		*/
		
	}
	
	@Override
	public void cleanup(Context context)throws IOException,InterruptedException
	{
		 SortedMap<Long,String> test = new TreeMap<Long,String>();
		 

		 while(tmap2.size()>5)
			{
				tmap2.remove(tmap2.lastKey());
			}
		 
		 
		 top_n_Mapper map = new top_n_Mapper();
		for(Map.Entry<Long,String> entry : tmap2.entrySet()) {
			  if((trial2<5)&&(map.trial<5)) {
			  long count = entry.getKey();         
			  String name = entry.getValue();
			  
			 
			  test.put(count, name);
			  if((test.size()>5)&&(test.size()>5))
				{
				  tmap2.remove(test.lastKey());
				  test.remove(test.lastKey());
				}else {
					int x=g.getNum();
					if(x<=5) {
						g.updateNum();
						context.write(new LongWritable(count),new Text(name));
						
					}
			  }
			  trial2=trial2+1;
			  }
			  
			}
		
	}
	
	
	
}