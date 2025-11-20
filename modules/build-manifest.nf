process buildManifest {
    container 'ghcr.io/project-defiant/qtlformer:latest'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'manifest.json'

    input:
    path input_path

    output:
    path 'manifest.json', emit: manifest_file

    script:
    """
    qtlformer manifest --input-path ${input_path} --output-path manifest.parquet
    """
}
