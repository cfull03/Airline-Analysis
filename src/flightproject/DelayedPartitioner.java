package flightproject;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DelayedPartitioner extends Partitioner<Text, FlightArray> implements Configurable {

    private Configuration configuration;

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public int getPartition(Text key, FlightArray value, int numPartitions) {
        Writable[] elements = value.get();
        if (elements.length > 2 && elements[2] instanceof BooleanWritable) {
            BooleanWritable delayed = (BooleanWritable) elements[2];
            return delayed.get() ? 0 : 1;
        } else {
            throw new IllegalArgumentException("Expected third element to be of type BooleanWritable");
        }
    }
}

