import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by hduser on 12/3/2016.
 */
public class TaggedJoiningGroupingComparator extends WritableComparator {


    public TaggedJoiningGroupingComparator() {
        super(TaggedKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TaggedKey taggedKey1 = (TaggedKey)a;
        TaggedKey taggedKey2 = (TaggedKey)b;
        return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
    }
}
