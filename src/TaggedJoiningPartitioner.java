import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by hduser on 12/3/2016.
 */
public class TaggedJoiningPartitioner extends Partitioner<TaggedKey,Text> {

    public void configure(JobConf job) {}

    @Override
    public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
        System.out.println("my print" + "Partitioner" );
        return taggedKey.getJoinKey().hashCode() % numPartitions;
    }
}
