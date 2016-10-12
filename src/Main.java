import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by hduser on 28/12/2015.
 */

public class Main {

    public static void main(String[] args) throws Exception {
        //My vars
        boolean firstTime =true;

        Splitter splitter = Splitter.on('/');
        StringBuilder filePaths = new StringBuilder();
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);

        /*
        //Delete output
        fs.delete(new Path("output/"), true);            //Delete output
        */

        //Get number of files
        Path pt = new Path("input/");
        ContentSummary cs = fs.getContentSummary(pt);
        long fileCount = cs.getFileCount();             //prosoxi sta hidden files

        //KeyFinder
        MyIndex myIndex ;

        Path path1 ;
        Path path2 ;

        Job job = null;

        for( int i=1 ; i < fileCount ; i++  ) {           //i < 6



            if(firstTime) {
                config.set(String.valueOf(i), "1");     //px file 1 -> proteraitotia 1
                config.set(String.valueOf(i+1), "2");
                filePaths.append("input/1").append(",");
                filePaths.append("input/2").append(",");
                path1 = new Path("input/1");
                path2 = new Path("input/2");
                firstTime = false ;
            }else{
                config.set(String.valueOf(i+1), "1");
                config.set("part-r-00000", "2");
                filePaths.append("input/"+String.valueOf(i+1)).append(",");
                filePaths.append(String.valueOf(i+fileCount-1)+"/"+"part-r-00000").append(",");
                path1 = new Path("input/"+String.valueOf(i+1));
                path2 = new Path(String.valueOf(i+fileCount-1)+"/"+"part-r-00000");
            }

            KeyFinder keyFinder = new KeyFinder();
            keyFinder.setPaths(path1,path2);
            myIndex = keyFinder.findKey(config, Splitter.on(" "));      //exw ta key values e.g "01"
            myIndex.packName1(path1.getName());
            myIndex.packName2(path2.getName());                         //key names e.g "keyIndex/1"

            String name1 ,name2 ;
            String index1 ,index2 ;
            name1 = myIndex.getName1();
            name2 = myIndex.getName2();
            index1 = myIndex.getStr_list1();
            index2 = myIndex.getStr_list2();

            config.set(name1,index1);      // "keyIndex/1" ,"0"
            config.set(name2,index2);      // "keyIndex/2" , "0"
            config.set("header",myIndex.getHeaderStr());
            config.set("separator", " ");
            config.set("mapreduce.output.textoutputformat.separator"," ");      //separator reducer

            filePaths.setLength(filePaths.length() - 1);
            //job = new Job(config, "ReduceSideJoin");
            job = Job.getInstance(config, "ReduceSideJoin");
            job.setJarByClass(CascadeJoin.class);

            job.setInputFormatClass(HeaderInputFormat.class);
            FileInputFormat.addInputPaths(job, filePaths.toString());
            FileOutputFormat.setOutputPath(job, new Path(String.valueOf(i+fileCount)));         //Output dir edw

            job.setMapperClass(CascadeJoin.MyMapper.class);
            job.setReducerClass(CascadeJoin.MyReducer.class);
            job.setPartitionerClass(TaggedJoiningPartitioner.class);
            job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
            job.setMapOutputKeyClass(TaggedKey.class);
            job.setMapOutputValueClass(FatValue.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
            job.waitForCompletion(true);

            //Clear Variables
            config = new Configuration();
            filePaths = new StringBuilder();
            job = null;
        }
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}