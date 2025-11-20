process buildManifest {
    container 'ubuntu:latest'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'manifest.json'

    input:
    path gcs_path

    output:
    path 'manifest.json', emit: manifest_file

    script:
    """
    ls -R ${gcs_path} > manifest.json
    """
}
