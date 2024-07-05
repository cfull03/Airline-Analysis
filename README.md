# Airline Delay Analysis using Hadoop and HBase

This project utilizes Apache Hadoop and HBase to perform large-scale data analysis on airline delay data. The main objective is to analyze flight delays and calculate the average delay times and distances for various airlines and airports. By leveraging Hadoop's distributed computing capabilities and HBase's efficient data storage and querying, this project can handle extensive datasets effectively.

## Features

- **MapReduce Job**: Custom mapper and reducer to process and aggregate airline delay data.
- **HBase Integration**: Storing aggregated delay data in HBase for efficient querying and analysis.
- **Custom Partitioner**: Partition data based on delay status for balanced processing.
- **Error Handling**: Robust error handling and validation to ensure data integrity.

## Project Structure

- **FlightMapper.java**: Mapper class to parse and map flight delay information.
- **AverageReducer.java**: Reducer class to aggregate delay data and store results in HBase.
- **DelayedPartitioner.java**: Custom partitioner to divide data based on delay status.
- **FlightArray.java**: Custom writable array to encapsulate flight delay data.
- **Main.java**: Main class to configure and run the Hadoop job.

## Prerequisites

- Java 8+
- Apache Hadoop
- Apache HBase
- Apache Maven

## Setup and Usage

1. **Clone the Repository**
    ```sh
    git clone https://github.com/your-username/airline-delay-analysis.git
    cd airline-delay-analysis
    ```

2. **Build the Project**
    ```sh
    mvn clean package
    ```

3. **Run the Hadoop Job**
    ```sh
    hadoop jar target/airline-delay-analysis-1.0.jar flightproject.Main <input_path> <output_path>
    ```

## Input Data Format

The input data should be in CSV format with the following columns:

FL_DATE,OP_CARRIER,OP_CARRIER_FL_NUM,ORIGIN,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,CRS_ELAPSED_TIME,ACTUAL_ELAPSED_TIME,AIR_TIME,DISTANCE,CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY,Unnamed: 27



## Example Output

The output will be stored in the specified HBase table `DelayedFlightData`, containing columns for:
- `Delayed`: Whether the flight was delayed.
- `AverageDelayedTime`: The average delay time.
- `AverageFlightDistance`: The average flight distance.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
