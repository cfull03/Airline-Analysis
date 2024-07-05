package flightproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageReducer extends Reducer<Text, FlightArray, Text, Text> {
	
	private Connection connection;
    private TableName tableName;
    private Table table;
	
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        tableName = TableName.valueOf("DelayedFlightData");

        if (!admin.tableExists(tableName)) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("DelayedFlightsData")).build())
                    .build();
            admin.createTable(tableDescriptor);
        }
        admin.close();
        table = connection.getTable(tableName);
    }
	
	@Override
	public void reduce(Text key, Iterable<FlightArray> values, Context context) 
	        throws IOException, InterruptedException {
		int timeSum = 0;
        int distanceSum = 0;
        int iterations = 0;
        boolean delayed = false;

        Text airline = null;
        Text airport = null;
		
		for (FlightArray value : values) {
            Writable[] elements = value.get();
            if (elements.length == 5) {
                airline = (Text) elements[0];
                airport = (Text) elements[1];
                BooleanWritable delayedWritable = (BooleanWritable) elements[2];
                FloatWritable delayedTimeWritable = (FloatWritable) elements[3];
                FloatWritable delayedDistanceWritable = (FloatWritable) elements[4];

                if (delayedWritable.get()) {
                    delayed = true;
                    timeSum += delayedTimeWritable.get();
                    distanceSum += delayedDistanceWritable.get();
                }
                iterations++;
            }
        }
            
		if (iterations > 0) {
            float averageTime = (float) timeSum / iterations;
            float averageDistance = (float) distanceSum / iterations;

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("DelayedFlightsData"), Bytes.toBytes("Delayed"), Bytes.toBytes(delayed));
            put.addColumn(Bytes.toBytes("DelayedFlightsData"), Bytes.toBytes("AverageDelayedTime"), Bytes.toBytes(averageTime));
            put.addColumn(Bytes.toBytes("DelayedFlightsData"), Bytes.toBytes("AverageFlightDistance"), Bytes.toBytes(averageDistance));

            table.put(put);

            FlightArray aggregatedData = new FlightArray(airline, airport, 
                    new BooleanWritable(delayed), 
                    new FloatWritable(averageTime), 
                    new FloatWritable(averageDistance));
            context.write(new Text("AggregatedData"), new Text(aggregatedData.toString()));
        }
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	    if (table != null) {
	        table.close();
	    }
	    if (connection != null) {
	        connection.close();
	    }
	    super.cleanup(context);
	}
}
