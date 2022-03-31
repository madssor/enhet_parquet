# This is a python script to create parquet file from enheter_alle.json.
# A file containing all legal entities in norway.
# The file can be found here: https://data.brreg.no/enhetsregisteret/oppslag/enheter/lastned
#
# The code is written by a non-python-programmer, and will be inefficient and slow.
# It should mostly be considered as a guide on how similar things could be done. (If you want it to be slow)
#
import gzip
import shutil
from io import BytesIO

import requests
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
# from memory_profiler import profile


# @profile
def convert_hovedenheter_to_parquet():
    # f = open("enheter_noen.json")
    f = open("enheter_alle.json")

    enheter = json.loads(f.read())

    df = pd.DataFrame(
        {
            'organisasjonsnummer': get_plain_prop_parq(enheter, "organisasjonsnummer"),
            'navn': get_plain_prop_parq(enheter, "navn"),
            'organisasjonsform.kode': get_level2_prop_parq(enheter, "organisasjonsform", "kode"),
            'organisasjonsform.beskrivelse': get_level2_prop_parq(enheter, "organisasjonsform", "beskrivelse"),
            'postadresse.landkode': get_level2_prop_parq(enheter, "postadresse", "landkode"),
            'postadresse.postnummer': get_level2_prop_parq(enheter, "postadresse", "postnummer"),
            'postadresse.poststed': get_level2_prop_parq(enheter, "postadresse", "poststed"),
            'postadresse.kommune': get_level2_prop_parq(enheter, "postadresse", "kommune"),
            'postadresse.kommunenummer': get_level2_prop_parq(enheter, "postadresse", "kommunenummer"),
            'registreringsdatoEnhetsregisteret': get_plain_prop_parq(enheter, "registreringsdatoEnhetsregisteret"),
            'registrertIMvaregisteret': get_plain_prop_parq(enheter, "registrertIMvaregisteret"),
            'naeringskode1.kode': get_level2_prop_parq(enheter, "naeringskode1", "kode"),
            'naeringskode1.hjelpeenhetskode': get_level2_bool_parq(enheter, "naeringskode1", "hjelpeenhetskode"),
            'naeringskode2.kode': get_level2_prop_parq(enheter, "naeringskode2", "kode"),
            'naeringskode2.hjelpeenhetskode': get_level2_bool_parq(enheter, "naeringskode2", "hjelpeenhetskode"),
            'naeringskode3.kode': get_level2_prop_parq(enheter, "naeringskode3", "kode"),
            'naeringskode3.hjelpeenhetskode': get_level2_bool_parq(enheter, "naeringskode3", "hjelpeenhetskode"),
            'antallAnsatte': get_plain_prop_parq(enheter, "antallAnsatte"),
            'forretningsadresse.landkode': get_level2_prop_parq(enheter, "forretningsadresse", "landkode"),
            'forretningsadresse.postnummer': get_level2_prop_parq(enheter, "forretningsadresse", "postnummer"),
            'forretningsadresse.poststed': get_level2_prop_parq(enheter, "forretningsadresse", "poststed"),
            'forretningsadresse.kommune': get_level2_prop_parq(enheter, "forretningsadresse", "kommune"),
            'forretningsadresse.kommunenummer': get_level2_prop_parq(enheter, "forretningsadresse", "kommunenummer"),
            'stiftelsesdato': get_plain_prop_parq(enheter, "stiftelsesdato"),
            'institusjonellSektorkode.kode': get_level2_prop_parq(enheter, "institusjonellSektorkode", "kode"),
            'registrertIForetaksregisteret': get_plain_prop_parq(enheter, "registrertIForetaksregisteret"),
            'registrertIStiftelsesregisteret': get_plain_prop_parq(enheter, "registrertIStiftelsesregisteret"),
            'registrertIFrivillighetsregisteret': get_plain_prop_parq(enheter, "registrertIFrivillighetsregisteret"),
            'konkurs': get_plain_prop_parq(enheter, "konkurs"),
            'underAvvikling': get_plain_prop_parq(enheter, "underAvvikling"),
            'underTvangsavviklingEllerTvangsopplosning': get_plain_prop_parq(enheter, "underTvangsavviklingEllerTvangsopplosning"),
            'maalform': get_plain_prop_parq(enheter, "maalform"),
         },
        index=get_plain_prop_parq(enheter, "organisasjonsnummer")
    )

    table = pa.Table.from_pandas(df)
    pq.write_table(table, "enheter.parquet")

    # table2 = pq.read_table("enheter.parquet")
    # print(table2.schema)
    # print(table2.columns)
    # print(table2.to_pandas())


def get_plain_prop_parq(json_records, property_name):
    result = []
    for json_record in json_records:
        if property_name in json_record:
            result.append(str(json_record[property_name]))
        else:
            result.append("")
    return result


def get_string_prop_parq(json_records, property_name):
    result = []
    for json_record in json_records:
        if property_name in json_record:
            result.append("\"" + str(json_record[property_name]) + "\"")
        else:
            result.append("")
    return result


def get_level2_prop_parq(json_records, property1, property2):
    result = []
    for json_record in json_records:
        if property1 in json_record and property2 in json_record[property1]:
            result.append((json_record[property1][property2]))
        else:
            result.append("")
    return result


def get_level2_bool_parq(json_records, property1, property2):
    result = []
    for json_record in json_records:
        if property1 in json_record and property2 in json_record[property1]:
            result.append((json_record[property1][property2]))
        else:
            result.append(False)
    return result


#
def download_enheter():
    print('Downloading started')
    response = requests.get("https://data.brreg.no/enhetsregisteret/api/enheter/lastned")
    print("Download complete")
    with gzip.open(BytesIO(response.content), "rb") as f_in:
        with open("enheter_alle.json", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    download_enheter()
    convert_hovedenheter_to_parquet()

