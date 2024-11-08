package mk.tradesense.tradesense.repository;

import mk.tradesense.tradesense.model.StockPrice;
import org.springframework.data.jpa.repository.JpaRepository;


public interface StockPriceRepository extends JpaRepository<StockPrice, Long> {
}