package ru.sandal.service;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.sandal.Product;

@Slf4j
@Component
public class ClickHouseService {

  private Connection conn;
  private static final String DB_URL = "jdbc:clickhouse://localhost:8123/default";

  public Connection getConnection() {
    try {
      return DriverManager.getConnection(DB_URL);
    } catch (SQLException e) {
      log.error("Connection is failed", e);
    }
    return null;
  }

  public String insertData(List<Product> products) {
    conn = getConnection();
    Date date = Date.valueOf(LocalDate.now());
    try (PreparedStatement pstmt = conn.prepareStatement(
        "INSERT INTO pet_clickhouse_grafana_base.products (* except (description))")) {

      conn.setAutoCommit(false);
      for (Product product : products) {
        pstmt.setInt(1, product.getId());
        pstmt.setString(2, product.getName());
        pstmt.setInt(3, product.getOrderCount());
        pstmt.setInt(4, product.getPrice());
        pstmt.setDate(5, date);
        pstmt.addBatch();
      }
      try {
        pstmt.executeBatch();
        conn.commit();
      } catch (BatchUpdateException ex) {
        log.error("ClickHouse batch ERROR" + ex);
        conn.rollback();
      } finally {
        conn.close();
      }
    } catch (SQLException e) {
      log.error("Clickhouse insert SQLException", e);
    }
    return "Clickhouse list size: " + products.size();
  }

  public void dropTable() throws SQLException {
    conn = getConnection();
    conn.createStatement().execute("drop table if exists pet_clickhouse_grafana_base.products;");
  }
}
