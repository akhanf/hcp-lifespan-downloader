import pandas as pd

#read dataframes
df_mri = pd.read_table(snakemake.input.img_manifest,sep='\t',header=[0],skiprows=[1])
df = pd.read_table(snakemake.input.data_manifest,sep='\t')

#get all files for subject
df_subj = df[df['associated_file'].str.contains(snakemake.wildcards.subject)]
    
#get dataset id for the job
dataset_id = df_mri.loc[df_mri['image_collection_name']==snakemake.params.package_name,'dataset_id'].unique()
    
#get files for this dataset and write to file
df_subj.loc[df['dataset_id'].isin(dataset_id.astype('str')),'associated_file'].to_csv(snakemake.output.txt_file,header=False,index=False)

