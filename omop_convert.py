import pandas as pd
from datetime import datetime

def transform_to_omop(file_path):

    df = pd.read_csv(file_path)
    
    # person table
    person_columns = [
        'person_id',
        'gender_concept_id',
        'year_of_birth',
        'month_of_birth',
        'day_of_birth',
        'birth_datetime',
        'race_concept_id',
        'ethnicity_concept_id',
        'location_id',
        'provider_id',
        'care_site_id',
        'person_source_value',
        'gender_source_value',
        'gender_source_concept_id',
        'race_source_value',
        'race_source_concept_id',
        'ethnicity_source_value',
        'ethnicity_source_concept_id'
    ]
    person = pd.DataFrame(columns=person_columns)
    
    # transform and populate
    person['person_id'] = df['PatientID'].apply(lambda x: abs(hash(x)) % (10 ** 10))
    person['gender_concept_id'] = df['PatientSex'].map({'M': 8507, 'F': 8532, 'O': 0})
    person['year_of_birth'] = df['PatientBirthDate'].apply(lambda x: str(x)[:4])
    person['race_concept_id'] = 0  # set as unknown
    person['ethnicity_concept_id'] = 0  # set as unknown
    person['gender_source_value'] = df['PatientSex']
    person['person_source_value'] = df['PatientID']
    
    # radilogy_occurrence table
    radiology_columns = [
        'radiology_occurrence_id',
        'person_id',
        'radiology_occurrence_date',
        'radiology_occurrence_datetime',
        'modality',
        'manufacturer',
        'protocol_concept_id',
        'protocol_source_value',
        'count_of_series',
        'count_of_images',
        'radiology_note',
        'referral_code',
        'referring_physician',
        'accession_id'
    ]
    radiology = pd.DataFrame(columns=radiology_columns)
    
    # transform + popualte
    radiology['radiology_occurrence_id'] = range(1, len(df) + 1)
    radiology['person_id'] = df['PatientID'].apply(lambda x: abs(hash(x)) % (10 ** 10))
    radiology['radiology_occurrence_date'] = pd.to_datetime(df['StudyDate'], format='%Y%m%d').dt.date
    radiology['modality'] = df['Modality']
    radiology['manufacturer'] = df['ManufacturerModelName']
    radiology['protocol_source_value'] = df['ProtocolName']
    radiology['referring_physician'] = df['ReferringPhysicianName']
    radiology['accession_id'] = df['AccessionNumber']
    radiology['radiology_note'] = 'synthetic_cxr_project - ' + df['conditioning']
    
    # person table may have duplicates...
    person = person.drop_duplicates(subset=['person_id'])
    
    return person, radiology

def save_tables(person_df, radiology_df):
    person_df.to_csv(f"omop/person.csv", index=False)
    radiology_df.to_csv(f"omop/radiology_occurrence.csv", index=False)

if __name__ == "__main__":
    person_table, radiology_table = transform_to_omop("data/synthetic_cxr_metadata.csv")
    
    save_tables(person_table, radiology_table)
    
    print("omop person:")
    print(person_table.head())
    print("omop radiology_occurrence:")
    print(radiology_table.head())