package flightproject;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable, Text, IntWritable, FlightArray> {

  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final int EXPECTED_COLUMNS = 28;

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] delayInfo = value.toString().split(",");

    // Ensure that there are enough columns
    if (delayInfo.length >= EXPECTED_COLUMNS) {
      try {
        LocalDate date = LocalDate.parse(delayInfo[0], DATE_FORMATTER);
        int year = date.getYear();

        String carrier = delayInfo[1].replace("\"", "");
        String origin = delayInfo[3].replace("\"", "");

        // Ensure necessary fields are not empty
        if (!delayInfo[7].isEmpty() && !delayInfo[21].isEmpty()) {
          float depDelay = Float.parseFloat(delayInfo[7]);
          float distance = Float.parseFloat(delayInfo[21]);

          context.write(new IntWritable(year), new FlightArray(
              new Text(carrier),
              new Text(origin),
              new BooleanWritable(true),
              new FloatWritable(depDelay),
              new FloatWritable(distance)
          ));
        }
      } catch (DateTimeParseException | NumberFormatException e) {
        e.printStackTrace();
      }
    }
  }
}
