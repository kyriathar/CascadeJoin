import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by hduser on 27/12/2015.
 */
public class CascadeJoin {


    public static class MyMapper extends Mapper<LongWritable, Text, TaggedKey, FatValue>{      //<input, input, output, output>
        private String fatKeyIndex;
        private List<String> indexList ;
        private Splitter splitter;
        private Joiner joiner;
        private TaggedKey taggedKey = new TaggedKey();
        private Text data = new Text();
        private int joinOrder;
        private FatValue fatValue = new FatValue();
        private ArrayList<String> emptyList = new ArrayList<>();

        private String findMyKeyIndex(Context context){
            StringBuilder builder = new StringBuilder();
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            builder.append("keyIndex/");
            builder.append(fileSplit.getPath().getName());
            return builder.toString() ;
        }

        private List<String> getIndex(String strIndex){
            List<String> list = Lists.newArrayList(splitter.split(strIndex));
            list.remove(list.get(list.size()-1));
            return list;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String separator = context.getConfiguration().get("separator");         //separator = " "
            splitter = Splitter.on(separator).trimResults();
            joiner = Joiner.on(separator);
            String myIndex = findMyKeyIndex(context);                               //e.g. "keyIndex/1" = "0"
            fatKeyIndex = context.getConfiguration().get(myIndex);
            indexList = getIndex(fatKeyIndex);
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
            emptyList.add("");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> values = Lists.newArrayList(splitter.split(value.toString()));     //kathe grammi
            String joinKey ;       //kathe grammi
            StringBuilder builder = new StringBuilder();
            for(String keyIndex : indexList){
                builder.append(values.get(Integer.parseInt(keyIndex))+" ");
                values.set(Integer.parseInt(keyIndex),"");
            }
            builder.setLength(builder.length() - 1);
            joinKey = builder.toString();
            values.removeAll(emptyList);
            String valuesWithOutKey = joiner.join(values);
            taggedKey.set(new Text(joinKey), new IntWritable(joinOrder));
            data.set(valuesWithOutKey);
            fatValue.set(data,joinOrder);

            context.write(taggedKey, fatValue);
        }
    }


    public static class MyReducer extends Reducer<TaggedKey,FatValue,Text,Text>{     //<input, input, output, output>

        private Text joinedText = new Text();
        private StringBuilder builder = new StringBuilder();
        private NullWritable nullKey = NullWritable.get();
        private List<String> firstList = new ArrayList();
        private List<String> secondList = new ArrayList();
        FatValue fatvalue ;
        private Text keyText = new Text();
        private Text combinedText = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(null,new Text(context.getConfiguration().get("header")));
        }

        @Override
        protected void reduce(TaggedKey key, Iterable<FatValue> values, Context context) throws IOException, InterruptedException {
            keyText.set(key.getJoinKey());

            Iterator<FatValue> iter = values.iterator() ;
            while(iter.hasNext()){
                fatvalue = iter.next();
                if( fatvalue.getJoinOrder() == 1  ){
                    firstList.add(fatvalue.getData().toString());       //stin 1i lista egrafes ap to 1o arxeio
                }
                else{
                    secondList.add(fatvalue.getData().toString());      //stin 2i lista egrafes ap to 2o arxeio
                }
            }

            for( String i : firstList ){             //join
                for( String j : secondList ){
                    if( i.isEmpty() )
                        combinedText.set( i+j );
                    else
                        combinedText.set( i+" "+j );
                    context.write(keyText,combinedText);
                }
            }
            firstList.clear();
            secondList.clear();

        }
    }
}