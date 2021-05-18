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
                subject=subjects,
                package=config['packages'].keys()),
    
rule create_s3_list:
    input:
        data_manifest = config['data_manifest'],
        img_manifest = config['img_manifest']
    params:
        package_name = lambda wildcards: config['packages'][wildcards.package]['name']
    output: 
        txt_file = 'results/s3_lists/{package}/{subject}_{package}.txt'
    script: 'create_s3_list.py'
        

rule filter_s3_list:
    input:
        txt_file = 'results/s3_lists/{package}/{subject}_{package}.txt'
    params:
        filters = lambda wildcards: config['packages'][wildcards.package]['filters']
    output:
        txt_file = 'results/s3_lists/{package}.filtered/{subject}_{package}.txt'
    run:
        for f in params.filters:
            shell('grep {f} {input.txt_file} >> {output.txt_file}')


def get_s3_txt(wildcards):
    if 'filters' in config['packages'][wildcards.package]:
        return 'results/s3_lists/{package}.filtered/{subject}_{package}.txt'.format(**wildcards)
    else:
        return 'results/s3_lists/{package}/{subject}_{package}.txt'.format(**wildcards)

rule download_package:
    input: 
        txt_file = get_s3_txt
    threads: 1
    shadow: 'minimal'
    output:
        dl_folder = directory('results/{package}/{subject}')
    shell: "if [ -s {input.txt_file} ];"
           " then "
           "  downloadcmd {input.txt_file} -d '.' -t -v -wt {threads} && "
           "  mv submission_*/* {output.dl_folder}; "
           " else "
           "  mkdir -p {output.dl_folder} && touch {output.dl_folder}/NO_FILES_IN_MANIFEST;"
           " fi"



