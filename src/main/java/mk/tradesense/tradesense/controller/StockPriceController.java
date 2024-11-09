package mk.tradesense.tradesense.controller;

import mk.tradesense.tradesense.model.StockPrice;
import mk.tradesense.tradesense.repository.StockPriceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/stock-prices")
public class StockPriceController {

    private final StockPriceRepository stockPriceRepository;

    @Autowired
    public StockPriceController(StockPriceRepository stockPriceRepository) {
        this.stockPriceRepository = stockPriceRepository;
    }

    @PostMapping
    public ResponseEntity<StockPrice> createStockPrice(@RequestBody StockPrice stockPrice) {
        StockPrice savedStockPrice = stockPriceRepository.save(stockPrice);
        return ResponseEntity.ok(savedStockPrice);
    }

    @GetMapping
    public ResponseEntity<List<StockPrice>> getAllStockPrices() {
        List<StockPrice> stockPrices = stockPriceRepository.findAll();
        return ResponseEntity.ok(stockPrices);
    }

    @GetMapping("/{id}")
    public ResponseEntity<StockPrice> getStockPriceById(@PathVariable Long id) {
        return stockPriceRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteStockPrice(@PathVariable Long id) {
        if (stockPriceRepository.existsById(id)) {
            stockPriceRepository.deleteById(id);
            return ResponseEntity.noContent().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }
}
