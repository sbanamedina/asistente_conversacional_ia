from extract import extract_siniestros


def main():
    """
    Extrae siniestros desde Postgres (stage).
    """
    print("Extrayendo siniestros...")   
    siniestros = extract_siniestros()
    print(f"Filas obtenidas: {len(siniestros)}")


if __name__ == "__main__":
    main()
