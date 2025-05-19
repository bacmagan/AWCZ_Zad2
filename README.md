# Rozwiązanie zadania 2

## Struktura projektu

```
├── main.py                                  # Główny plik zawierający wszystkie funkcje
├── generator.py                             # Generator danych strumieniowych do katalogu data/stream
├── rate_jako_zrodlo_strumieniowana.py
├── filtrowanie_danych_bez_agregacji.py
├── zrodlo_plikowe.py
├── bezstanowe_zliczanie_zdarzen.py
├── agregacja_w_oknach_czasowych_tumbling.py
├── agregacja_w_oknach_czasowych_sliding.py
├── segmentacja_klientow.py
├── data/
│   └── stream/                              # Katalog na dane wejściowe (generowane pliki JSON)
├── checkpoint/                              # Checkpointy Spark dla zachowania stanu między batchami
└── hadoop/
    └── bin/
        └── winutils.exe                     # Plik wymagany do działania Spark na Windowsie
```

## Jak uruchomić?

### 1. Generowanie danych wejściowych

Uruchamianie generatora danych:

```bash
python generator.py
```

Skrypt co 5 sekund zapisuje pliki JSON zawierające 50 zdarzeń do katalogu `data/stream/`.

### 2. Analiza danych

Uruchom analizę danych z pliku `main.py`, odkomentowując odpowiednią funkcję na końcu pliku:

```python
# bezstanowe_zliczanie_zdarzen()
# zrodlo_plikowe()
# rate_jako_zrodlo_strumieniowana()
# filtrowanie_danych_bez_agregacji()
# agregacja_w_oknach_czasowych_tumbling()
# agregacja_w_oknach_czasowych_sliding()
# segmentacja_klientow()
```

Alternatywnie można uruchamiać każdą funkcję osobno z odpowiadającego jej pliku `.py`.

### 3. Hadoop na Windows

Aby uniknąć problemów z HDFS na Windowsie, projekt zawiera folder `hadoop/` z `winutils.exe`. Skrypt automatycznie ustawia `HADOOP_HOME`, jeśli jest uruchamiany na systemie Windows.
