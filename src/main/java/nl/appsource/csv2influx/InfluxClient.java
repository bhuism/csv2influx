package nl.appsource.csv2influx;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class InfluxClient implements Consumer<Map<String, Double>> {

    final String url;
    final String username;
    final String password;

    final String dbName = "stock";

    InfluxDB influxDB;
    
    int i = 0;

    public void connect() {
        
        if (influxDB != null) {
            close();
        }

        influxDB = InfluxDBFactory.connect(url, username, password);

        influxDB.setDatabase(dbName);

        influxDB.enableGzip();
        
        influxDB.enableBatch(BatchOptions.DEFAULTS.exceptionHandler((failedPoints, e) -> {
            log.error("", e);
        })
            .actions(300)
            .flushDuration((int) Duration.ofMillis(250).toMillis())
          );
        
        log.info("Influxdb.version " + influxDB.version());
        

    }

    @Override
    public void accept(final Map<String, Double> row) {
        
//        log.debug("Wrting " + (i++) + " timestamp=" + row.get("timestamp").longValue());
        
        final Builder builder = Point.measurement("historical2")
                .time(row.get("timestamp").longValue(), TimeUnit.SECONDS);

        row.forEach((k, v) -> {
            builder.addField(k, v);
        });

        final BatchPoints batchPoints = BatchPoints.database(dbName).build();
        batchPoints.point(builder.build());
        influxDB.write(batchPoints);
    }

    public void close() {
        if (influxDB != null) {
            try {
                influxDB.flush();
                influxDB.close();
            } catch (Exception e) {
                log.error("influxDB.close() failed", e);
            } finally {
                influxDB = null;
            }
        }
    }

}
