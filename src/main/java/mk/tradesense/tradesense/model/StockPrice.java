package mk.tradesense.tradesense.model;

import jakarta.persistence.*;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@Entity
@Table(name = "stock_prices")
public class StockPrice {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "stock_code", nullable = false)
    private String stockCode;

    @Column(nullable = false)
    private LocalDate date;

    private BigDecimal lastPrice;
    private BigDecimal maxPrice;
    private BigDecimal minPrice;
    private BigDecimal avgPrice;
    private BigDecimal percentChange;
    private Integer quantity;
    private BigDecimal turnoverBest;
    private BigDecimal totalTurnover;


}
