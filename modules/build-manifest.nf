process buildManifest {
    container 'ghcr.io/project-defiant/qtlformer:0.2.4'
    machineType 'n1-standard-4'
    time '10m'
    debug true
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'manifest.tsv'

    input:
    path input_path

    output:
    path 'manifest.tsv', emit: manifest_file

    script:
    """
    qtlformer manifest ${input_path} manifest.tsv
    """
}
