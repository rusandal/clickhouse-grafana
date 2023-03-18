package ru.sandal;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import ru.sandal.config.ClickhouseConfig;
import ru.sandal.service.ClickHouseService;

@Slf4j
@Component
public class StarterApplication implements CommandLineRunner {

  @Autowired
  private ClickhouseConfig clickhouseConfig;
  @Autowired
  private ClickHouseService clickHouseService;

  @Override
  public void run(String... args) throws Exception {
    clickhouseConfig.createDatabase();
    Product product1 = Product.builder().id(1).name("product name").orderCount(22).price(123)
        .build();
    Product product2 = Product.builder().id(2).name("product2 name").orderCount(100).price(123)
        .build();
    String insertData = clickHouseService.insertData(List.of(product1, product2));
    log.info("Products was inserted." + insertData);
    clickHouseService.dropTable();
    log.info("The table was dropped");

  }
}
