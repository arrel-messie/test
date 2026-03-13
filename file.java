package xxxx

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Construit le rapport en trois passes d'agrégation :
 *   1. Lecture du JSON bas niveau (par PSP)
 *   2. Agrégation PSP -> CountryKpi
 *   3. Agrégation CountryKpi -> EcbKpi (niveau rapport)
 */
public class ReportBuilder {

    private final ObjectMapper mapper;

    public ReportBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    // -------------------------------------------------------------------------
    // Point d'entrée principal
    // -------------------------------------------------------------------------

    public List<EcbKpi> buildFrom(InputStream pspLevelJson) throws Exception {
        List<PspLevelEntry> rawEntries = readPspEntries(pspLevelJson);
        return buildEcbKpis(rawEntries);
    }

    // -------------------------------------------------------------------------
    // Étape 1 : lecture Jackson
    // -------------------------------------------------------------------------

    private List<PspLevelEntry> readPspEntries(InputStream json) throws Exception {
        return mapper.readValue(json, new TypeReference<List<PspLevelEntry>>() {});
    }

    // -------------------------------------------------------------------------
    // Étape 2 : PSP -> CountryKpi
    // Groupement par (reportDate, currency, country), puis agrégation des PSPs
    // -------------------------------------------------------------------------

    private List<CountryKpi> buildCountryKpis(List<PspLevelEntry> entries) {
        // Groupement : (reportDate, currency, country) -> liste d'entrées PSP
        Map<CountryKey, List<PspLevelEntry>> byCountry = entries.stream()
                .collect(Collectors.groupingBy(e ->
                        new CountryKey(e.reportDate(), e.currency(), e.country())));

        return byCountry.entrySet().stream()
                .map(entry -> aggregateToCountry(entry.getKey(), entry.getValue()))
                .toList();
    }

    private CountryKpi aggregateToCountry(CountryKey key, List<PspLevelEntry> pspEntries) {
        // Agrégation des montants country
        double online        = sum(pspEntries, e -> e.amountOnline());
        double offline       = sum(pspEntries, e -> e.amountOffline());
        double fundedOnline  = sum(pspEntries, e -> e.amountFundedOnline());
        double fundedOffline = sum(pspEntries, e -> e.amountFundedOffline());
        double defOnline     = sum(pspEntries, e -> e.amountDefundedOnline());
        double defOffline    = sum(pspEntries, e -> e.amountDefundedOffline());

        double overall         = online + offline;
        double fundedOverall   = fundedOnline + fundedOffline;
        double defundedOverall = defOnline + defOffline;
        double netFunded       = fundedOverall - defundedOverall;

        // Construction des PspKpi enfants
        List<PspKpi> dataPsp = pspEntries.stream()
                .map(e -> new PspKpi(
                        e.psp().bic(),
                        e.psp().name(),
                        e.amountOnline(),
                        e.amountOffline()))
                .toList();

        return new CountryKpi(
                key.country(),
                overall,
                online,
                offline,
                fundedOverall,
                fundedOnline,
                fundedOffline,
                defundedOverall,
                defOnline,
                defOffline,
                netFunded,
                defundedOverall,
                dataPsp
        );
    }

    // -------------------------------------------------------------------------
    // Étape 3 : CountryKpi -> EcbKpi
    // Groupement par (reportDate, currency), puis agrégation des countries
    // -------------------------------------------------------------------------

    private List<EcbKpi> buildEcbKpis(List<PspLevelEntry> rawEntries) {
        // On passe par les CountryKpi d'abord
        List<CountryKpi> allCountries = buildCountryKpis(rawEntries);

        // Pour reconstituer le groupement (reportDate, currency) -> countries,
        // on s'appuie sur les entries brutes comme clé
        Map<EcbKey, List<PspLevelEntry>> byEcb = rawEntries.stream()
                .collect(Collectors.groupingBy(e -> new EcbKey(e.reportDate(), e.currency())));

        return byEcb.entrySet().stream()
                .map(entry -> {
                    EcbKey ecbKey = entry.getKey();

                    // Récupération des CountryKpi qui correspondent à cette (date, devise)
                    List<CountryKpi> countriesForThisEcb = allCountries.stream()
                            .filter(c -> entry.getValue().stream()
                                    .anyMatch(e -> e.country().equals(c.country())))
                            .toList();

                    double totalOnline  = countriesForThisEcb.stream()
                            .mapToDouble(CountryKpi::totalCountryAmountOnline).sum();
                    double totalOffline = countriesForThisEcb.stream()
                            .mapToDouble(CountryKpi::totalCountryAmountOffline).sum();

                    return new EcbKpi(
                            ecbKey.reportDate(),
                            ecbKey.currency(),
                            totalOnline + totalOffline,
                            totalOnline,
                            totalOffline,
                            countriesForThisEcb
                    );
                })
                .toList();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    @FunctionalInterface
    private interface ToDouble<T> {
        double apply(T t);
    }

    private <T> double sum(List<T> list, ToDouble<T> extractor) {
        return list.stream().mapToDouble(extractor::apply).sum();
    }

    // Clés de groupement (records Java 21 → equals/hashCode auto)
    private record CountryKey(String reportDate, String currency, String country) {}
    private record EcbKey(String reportDate, String currency) {}
}
