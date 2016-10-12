import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by hduser on 16/3/2016.
 */
public class FatValue implements Writable {
    private Text data = new Text();
    private int joinOrder;  //sto reducer wste sto joining na ksexwrisei tis eggrafes tou file1 apo tou file2

    public void set(Text data ,int joinOrder){
        this.data = data ;
        this.joinOrder = joinOrder ;
    }

    public Text getData(){
        return data ;
    }

    public int getJoinOrder(){
        return joinOrder ;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //joinKey.readFields(in);
        data.set(WritableUtils.readString(in));
        //tag.readFields(in);
        joinOrder = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //joinKey.write(out);
        WritableUtils.writeString(out, String.valueOf(data));
        //tag.write(out);
        out.writeInt(joinOrder);
    }
}
