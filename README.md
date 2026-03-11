
Get started by customizing your environment (defined in the .idx/dev.nix file) with the tools and IDE extensions you'll need for your project!

Learn more at https://developers.google.com/idx/guides/customize-idx-env

## Installation

To install the required packages, you'll need to have Python installed and accessible from your terminal.

### Running the installation command:

In your terminal, run the following command:

```bash
python -m pip install -r requirements.txt
```

### Troubleshooting

**If you get a "'python' is not recognized..." error on Windows:**

This means that Python is not in your system's PATH. You can do one of the following:

1.  **Use the full path to your Python executable.** If you installed Python in `C:\Python313`, for example, the command would be:
    ```bash
    C:\Python313\python.exe -m pip install -r requirements.txt
    ```

2.  **Add Python to your PATH.** You can find instructions on how to do this in the official Python documentation for Windows. This will allow you to use the `python` command directly from any terminal.
