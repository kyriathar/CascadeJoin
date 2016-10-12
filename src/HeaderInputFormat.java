import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class HeaderInputFormat extends FileInputFormat<LongWritable, Text>{
     Text header;

     /**
      * We dont split files when files have headers.  We need to read the 
      * header in the init method.
     
     @Override
     protected boolean isSplitable(JobContext context, Path file) {
         return false;
     }*/

     
        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,TaskAttemptContext arg1) throws IOException, InterruptedException{
            return new LineRecordReader(){
                @Override
                public void initialize(InputSplit genericSplit,TaskAttemptContext context) throws IOException{
                    super.initialize(genericSplit, context);
                    /**
                     * Read the first line.
                     */
		    FileSplit split = (FileSplit) genericSplit;
		    if (split.getStart() == 0)
		    {		System.out.println("First split BRO!!!!! - in recordread initializer setup");
                    		super.nextKeyValue();
		    }

                 //   header = super.getCurrentKey();
                  //  System.out.println("header = "+ header);
                }
                
        /*        @Override
                public Text getCurrentKey(){
                    // TODO Auto-generated method stub
                    return this.getCurrentValue();
                }*/

                /**
                 * Starts at line 2.  First line is the header. 
                 */
         /*       @Override
                public Text getCurrentValue(){
                    return new Text(super.getCurrentKey().toString());
                }
             */   
            };
        }
 }
