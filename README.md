## Environment Variables

Create a `.env` file in the root directory with the following variables:

```plaintext
DB_NAME="db name here"
DB_USER="db username here"
DB_PASSWORD="db password here"
DB_HOST="db host here"
DB_PORT="db port here"
PYTHON_PATH="python path here"

### Summary

1. **Install** `python-dotenv`.
2. **Create** a `.env` file with your environment variables.
3. **Add** `.env` to `.gitignore`.
4. **Load** environment variables using `load_dotenv()` and access them via `os.getenv()`.
5. **Use** environment variables in Java code via `System.getenv()`.
6. **Document** the required environment variables in your README.

Using `python-dotenv` with a `.env` file provides a secure and modular way to manage sensitive information in your project. Let me know if you have any questions or need further assistance!
