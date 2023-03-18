package ru.sandal.config;

import java.sql.SQLException;
import java.sql.Statement;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.sandal.service.ClickHouseService;

@Slf4j
@NoArgsConstructor
@Component
public class ClickhouseConfig {

  @Autowired
  private ClickHouseService clickHouseService;
  private static final Integer BATCH_SIZE = 1000000;

  public void createDatabase() {
    String createDatabase = "CREATE DATABASE IF NOT EXISTS pet_clickhouse_grafana_base;";
    String createTableOrderCountAndPrice = "CREATE TABLE IF NOT EXISTS pet_clickhouse_grafana_base.products "
        + "(id_product UInt64, name_product String, count UInt64, price UInt64, i_date Date) ENGINE = MergeTree() ORDER BY id_product partition by toYYYYMM(i_date);";
    try (Statement statement = clickHouseService.getConnection().createStatement()) {
      statement.addBatch(createDatabase);
      statement.addBatch(createTableOrderCountAndPrice);
      statement.executeBatch();
      log.info("BD pet_clickhouse_grafana_base was created");
    } catch (SQLException e) {
      log.error("DB pet_clickhouse_grafana_base SQLException", e);
    }

  }


}
