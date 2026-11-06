import java.util.Comparator;
import java.util.List;

public class Soluciones {
    public record Repuesto(String nombre, String categoria, double precio, int stock) {}

    public static void main(String[] args) {
        List<Repuesto> repuestos = List.of(
            new Repuesto("Pastillas de Freno", "Frenos", 25.0, 15),
            new Repuesto("Disco de Freno", "Frenos", 45.0, 8),
            new Repuesto("Amortiguador Delantero", "Suspension", 65.0, 4),
            new Repuesto("Espiral Trasero", "Suspension", 50.0, 6),
            new Repuesto("Filtro de Aceite", "Motor", 8.0, 30),
            new Repuesto("Filtro de Aire", "Motor", 12.0, 20),
            new Repuesto("Bujia Iridium", "Motor", 10.0, 40),
            new Repuesto("Kit de Embrague", "Transmision", 120.0, 3),
            new Repuesto("Aceite 10W40", "Lubricantes", 28.0, 18),
            new Repuesto("Refrigerante", "Refrigeracion", 15.0, 12)
        );

        // 1. Selección (filter)
        List<Repuesto> repuestosCriticos = repuestos.stream()
            .filter(r -> r.stock() < 5)
            .toList();

        // 2. Transformación (map)
        List<String> preciosFacturacion = repuestos.stream()
            .map(r -> r.nombre().toUpperCase() + " -> $" + String.format("%.2f", r.precio() * 1.15))
            .toList();

        // 3. Extremos (max)
        Repuesto repuestoMasCaro = repuestos.stream()
            .max(Comparator.comparingDouble(r -> r.precio()))
            .orElseThrow();

        // 4. Acumulación (reduce)
        double totalInventario = repuestos.stream()
            .reduce(0.0, (acc, r) -> acc + (r.precio() * r.stock()), Double::sum);
    }
}
