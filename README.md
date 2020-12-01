Spout - le arquivo e envia cada uma das linhas para um Bolt
Bolt - faz o processamento contando as palavras que chegam

Main - paralelismo 2 - 2 Bolts para processar

allGrouping - todos os bolts recebem todas as infs.
Customizado (alphaGrouping) - palavras que começam com a em um bolt e as demais em outro.
shuffleGrouping - conta aleatório.
FieldsGrouping - palavras repetidas no mesmo Bolt.
