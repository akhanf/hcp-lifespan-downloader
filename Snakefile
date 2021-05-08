import pandas as pd

configfile: 'config.yml'



#get subject ids
df_mri = pd.read_table(config['img_manifest'],sep='\t',header=[0],skiprows=[1])
subjects = [ f'{subject}_V1_MR' for subject in df_mri['src_subject_id'].unique()]

wildcard_constraints:
    package = '[a-zA-Z0-9_]+',
    subject = '[a-zA-Z0-9_]+'


rule all:
    input: 
        dirs = expand('results/{package}/{subject}',
                subject=subjects[0],
                package=config['packages'].keys())

rule create_s3_list:
    input:
        data_manifest = config['data_manifest'],
        img_manifest = config['img_manifest']
    params:
        package_name = lambda wildcards: config['packages'][wildcards.package]
    output: 
        txt_file = 'results/s3_lists/{package}/{subject}_{package}.txt'
    script: 'create_s3_list.py'
        

rule download_package:
    input: 
        txt_file = 'results/s3_lists/{package}/{subject}_{package}.txt'
    threads: 1
    shadow: 'minimal'
    output:
        dl_folder = directory('results/{package}/{subject}')
    shell: "downloadcmd {input.txt_file} -d '.' -t -v -wt {threads} && "
           " mv submission_*/* {output.dl_folder}"