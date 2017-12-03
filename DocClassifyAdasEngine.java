import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DocClassifyAdasEngine {
		public static class TokenizerMapper
	       extends Mapper<Object, Text, Text, Text>{
			// Classifications nicknames
			// AdasEngine (A), DijkstrasPath (D), GargsDistribution (G), LamportsLock (L)
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
		    	  
		      context.write(mark, text);
		      }
		    }
		  }

		  public static class ClassifyLine
		       extends Reducer<Text,Text,Text,Text> {
		    private Text result = new Text();
		    private Text myClassification = new Text("AdasEngine");
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
		    					// Unmarked - so display only U
		    					result = val;
		    				} else {
		    					// Hide A, D, G, L
		    					result = hidden;
		    				}
		    			} else if (myClassification.toString().equals(classificationMarkingA.toString())){
		    				if((key.toString().equals(classificationMarkingA.toString()))||
		    						(key.toString().equals(classificationMarkingU.toString()))) {
		    					// Level 1 - need to display only A, U
		    					result = val;
		    				} else {
		    					// Hide D, G, L
		    					result = hidden;
		    				}
		    			} else if (myClassification.toString().equals(classificationMarkingD.toString())){
		    				if ((key.toString().equals(classificationMarkingA.toString())) || 
		    						(key.toString().equals(classificationMarkingD.toString())) ||
		    						(key.toString().equals(classificationMarkingU.toString()))) {
		    					// Level 2 - need to display only A, D, U
		    					result = val;
		    				} else {
		    					// Hide Class G, L
		    					result = hidden;
		    				}
		    			} else if (myClassification.toString().equals(classificationMarkingG.toString())){
		    				if ((key.toString().equals(classificationMarkingA.toString())) ||
		    						(key.toString().equals(classificationMarkingD.toString())) ||
		    						(key.toString().equals(classificationMarkingG.toString())) ||
		    						(key.toString().equals(classificationMarkingU.toString()))) {
		    					// Level 3 - need to display only A, D, G, U
		    					result = val;
		    				} else {
		    					// Hide Class L
		    					result = hidden;
		    				}
		    			} else if (myClassification.toString().equals(classificationMarkingL.toString())) {
		    				// Level 4 - need to display only A, D, G, L, U
		    				result = val;
		    			} 

		    			context.write(key, result);
		    		}
		    }
		  }

		  public static void main(String[] args) throws Exception {
			    Configuration conf = new Configuration();
			    Job job = Job.getInstance(conf, "word count");
			    job.setJarByClass(DocClassifyAdasEngine.class);
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
