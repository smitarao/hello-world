import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class DocClassifyDijkstrasPath {
		// Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
		public static class TokenizerMapper
	       extends Mapper<Object, Text, Text, Text>{
			// Classifications nicknames
			// AdasEngine (A), DijkstrasPath (D), GargsDistribution (G), LamportsLock (L)

		    //private final static IntWritable one = new IntWritable(1);
			private Text mark = new Text();
		    private Text line = new Text();
		    private Text text = new Text();

		    public void map(Object key, Text value, Context context
		                    ) throws IOException, InterruptedException {
		      StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
		      while (itr.hasMoreTokens()) {
		    	  line.set(itr.nextToken());
		    	  
		    	  // Checking if the line has a classification. If yes, splitting
		    	  String origLine = line.toString();
		    	  String textLine;
		    	  String markLine;
		    	  if (origLine.startsWith("(A)")){
		    		  markLine = "AdasEngine";	// Setting marking for the line
		    		  textLine = origLine.substring(3, origLine.length()-1);	// Setting the text for the line 
		    		  
		    		  mark.set(markLine);
		    		  text.set(textLine);
		    	  } 
		    	  else if (origLine.startsWith("(G)")){
		    		  markLine = "GargsDistribution";	// Setting marking for the line
		    		  textLine = origLine.substring(3, origLine.length()-1);	// Setting the text for the line 
		    		  
		    		  mark.set(markLine);
		    		  text.set(textLine);
		    	  } else if (origLine.startsWith("(L)")){
		    		  markLine = "LamportsLock";	// Setting marking for the line
		    		  textLine = origLine.substring(3, origLine.length()-1);	// Setting the text for the line 
		    		  
		    		  mark.set(markLine);
		    		  text.set(textLine);
		    	  } else if (origLine.startsWith("(D)")){
		    		  markLine = "DijkstrasPath";	// Setting marking for the line
		    		  textLine = origLine.substring(3, origLine.length()-1);	// Setting the text for the line 
		    		  
		    		  mark.set(markLine);
		    		  text.set(textLine);
		    	  } else {
		    		  markLine = "Unmarked";
		    		  textLine = origLine;
		    		  
		    		  mark.set(markLine);
		    		  text.set(textLine);
		    	  }
		    	  System.out.println("SmitaDebug: " + markLine + ", " + textLine);
		      context.write(mark, text);
		      }
		    }
		  }

		  public static class ClassifyLine
		       extends Reducer<Text,Text,Text,Text> {
		    private Text result = new Text();
		    
		    private Text myClassification = new Text("DijkstrasPath");
		    
		    private Text classificationMarkingA = new Text("AdasEngine");
		    private Text classificationMarkingD = new Text("DijkstrasPath");
		    private Text classificationMarkingG = new Text("GargsDistribution");
		    private Text classificationMarkingL = new Text("LamportsLock");
		    private Text classificationMarkingU = new Text("Unmarked");
		    
		    private Text hidden = new Text("**********");

		    public void reduce(Text key, Iterable<Text> values,
		                       Context context
		                       ) throws IOException, InterruptedException {
		    		for (Text val: values) {
		    			
		    			if (myClassification.toString().equals(classificationMarkingU.toString())){		
		    				if (key.toString().equals(classificationMarkingU.toString())){
		    					// Unmarked - so display only classificationMarkingU
		    					result = val;
		    				} else {
		    					// Hide classificationMarkingA, classificationMarkingD, classificationMarkingG, classificationMarkingL
		    					result = hidden;
		    				}
		    				//System.out.println("SmitaDebug: U: " + key + ", " + result);
		    			} else if (myClassification.toString().equals(classificationMarkingA.toString())){
		    				if((key.toString().equals(classificationMarkingA.toString()))||
		    						(key.toString().equals(classificationMarkingU.toString()))) {
		    					// Level 1 - need to display only classificationMarkingA, classificationMarkingU
		    					result = val;
		    				} else {
		    					// Hide classificationMarkingD, classificationMarkingG, classificationMarkingL
		    					result = hidden;
		    				}
		    				//System.out.println("SmitaDebug: A: " + key + ", " + result);
		    			} else if (myClassification.toString().equals(classificationMarkingD.toString())){
		    				if ((key.toString().equals(classificationMarkingA.toString())) || 
		    						(key.toString().equals(classificationMarkingD.toString())) ||
		    						(key.toString().equals(classificationMarkingU.toString()))) {
		    					// Level 2 - need to display only classificationMarkingA, classificationMarkingD, classificationMarkingU
		    					result = val;
		    				} else {
		    					// Hide Class G, L
		    					result = hidden;
		    				}
		    				//System.out.println("SmitaDebug: D: " + key + ", " + result);
		    			} else if (myClassification.toString().equals(classificationMarkingG.toString())){
		    				if ((key.toString().equals(classificationMarkingA.toString())) ||
		    						(key.toString().equals(classificationMarkingD.toString())) ||
		    						(key.toString().equals(classificationMarkingG.toString())) ||
		    						(key.toString().equals(classificationMarkingU.toString()))) {
		    					// Level 3 - need to display only classificationMarkingA, classificationMarkingD, classificationMarkingG, classificationMarkingU
		    					result = val;
		    				} else {
		    					// Hide Class L
		    					result = hidden;
		    				}
		    				//System.out.println("SmitaDebug: G: " + key + ", " + result);
		    			} else if (myClassification.toString().equals(classificationMarkingL.toString())) {
		    				// Level 4 - need to display only classificationMarkingA, classificationMarkingD, classificationMarkingG, classificationMarkingL, classificationMarkingU
		    				result = val;
		    				//System.out.println("SmitaDebug: L: " + key + ", " + result);
		    			} 
		    			
		    			/*
		    			if (myClassification.toString().equals(key.toString())) { 
		    				result = val;
		    			}
		    			else {
		    				result = hidden;
		    			}
		    			*/
		    			//System.out.println("SmitaDebug: " + key + ", " + result); // Debug statement
					context.write(key, result);
		    		}
		    }
		  }

		  public static void main(String[] args) throws Exception {
			    Configuration conf = new Configuration();
			    Job job = Job.getInstance(conf, "word count");
			    job.setJarByClass(DocClassifyDijkstrasPath.class);
			    job.setMapperClass(TokenizerMapper.class);
			    job.setCombinerClass(ClassifyLine.class);
			    job.setReducerClass(ClassifyLine.class);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(Text.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
