import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();            //(joinOrder)wste ston reducer na valei tou idiou key ,auta pou einai tou file1 prin to fiel2

     @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if(compareValue == 0 ){
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
        return compareValue;
    }

    public void set (Text joinKey  , IntWritable tag ) {this.joinKey = joinKey; this.tag = tag;}

    public void setJoinKey (Text joinKey ){this.joinKey = joinKey;}

    public void setTag (IntWritable tag  ){this.tag = tag;}

    public Text getJoinKey() {
        return joinKey;
    }

    public IntWritable getTag() {
        return tag;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //joinKey.readFields(in);
        joinKey.set(WritableUtils.readString(in));
        //tag.readFields(in);
        tag.set(in.readInt());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //joinKey.write(out);
        WritableUtils.writeString(out, String.valueOf(joinKey));
        //tag.write(out);
        out.writeInt(tag.get());
    }
}
