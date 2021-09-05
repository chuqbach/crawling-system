class LogException:

    def __init__(self, func, logfile='out.log'):
        functools.update_wrapper(self, func)
        self.func = func
        self.logfile = logfile

    def __call__(self, *args, **kwargs):
        log_string = self.func.__name__ + " was called"
        print(log_string)

        try:
            return self.func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger('SmartfileTest')

            file_logger = logging.FileHandler('logging_exception.log')

            NEW_FORMAT = f'[%(asctime)s] - [%(levelname)s] - %(message)s'

            file_logger_format = logging.Formatter(NEW_FORMAT)

            file_logger.setFormatter(file_logger_format)

            logger.setLevel(logging.DEBUG)

            logger.addHandler(file_logger)

            logger.warning(f"Bug: {e}")
            logger.info(f"Bug Type: {type(e).__name__}")
            logger.info(f"Bug Message: {str(e).title()}")
            logger.info(f"File Path: {e.__traceback__.tb_frame.f_code.co_filename}")
            logger.info(f"Line Number: {e.__traceback__.tb_lineno}")
            logger.info("========================= End Bug =======================")

    def notify(self):
        # đây là class log file thường nên sẽ không implement chức năng gửi mail
        pass