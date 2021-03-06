package nl.appsource.csv2influx;

import java.awt.Choice;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

@Slf4j
@SpringBootApplication
public class Csv2Influx implements CommandLineRunner {

    @Value("${url}")
    private String url;

    @Value("${username}")
    private String username;

    @Value("${password}")
    private String password;

    final Path path = FileSystems.getDefault().getPath("/tmp/btceUSD.csv");
    
    @PostConstruct
    public void postConstruct() {
    }
    
    public static <K, V> Map<K, V> toMap(final List<K> columns, final V[] row) {
        return IntStream.range(0, columns.size()).collect(HashMap::new, (m, i) -> m.put(columns.get(i), row[i]), Map::putAll);
    }
    
    @Override
    public void run(String... args) throws Exception {
        
        log.info("Start");
        
        final List<String> columns = fromPath(path)
            .map(line -> line.split(","))
            .map(Arrays::asList)
            .blockFirst()
            ;

        log.info("headers: " + columns);
        
        final AtomicInteger count = new AtomicInteger();

        Disposable interval = Flux.interval(Duration.ofSeconds(1)).subscribe((seconds) -> {
            log.debug("Speed: " + count.getAndSet(0) + " inserts / sec");
        });
        
        final InfluxClient influxClient = new InfluxClient(url, username, password);
        influxClient.connect();
        
        fromPath(path)
                .skip(1)
                .map(line -> line.split(","))
                .doOnNext((l) -> {
                    count.incrementAndGet();
                })
                .map((r) -> toMap(columns, r))
//                .log()
//                .subscribe(System.out::println)
                .subscribe(influxClient)
                .dispose();
        ;
        
        Collectors.toMap(Choice::getName, Function.identity());
        
        influxClient.close();

        interval.dispose();

        log.info("Done");

    }

    public static void main(String[] args) {
        final ConfigurableApplicationContext context = new SpringApplicationBuilder(Csv2Influx.class)
            .bannerMode(Mode.OFF)
            .web(WebApplicationType.NONE)
            .run(args)
            ;
            
        System.exit(SpringApplication.exit(context));
    }

    private static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                        Flux::fromStream,
                        BaseStream::close
        );
    }
    
}
