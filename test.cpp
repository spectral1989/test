
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <atomic>
#include <string.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <mutex>

using namespace std;

#define SETTINGS_PORT				5555
#define TRANSLATION_PORT			5556

#define DATA_SEGMENT_SIZE_READ		16384		// 64k max!


enum
{
	READ_NONE,
	READ_PROCESS,
	READ_ENDED,
	READ_END_HANDLED
};

enum
{
	TRANSLATION_NONE,
	TRANSLATION_IN_PROCESS
};

enum
{
	READ_NONBLOCKED,
	READ_BLOCKED
};

// settings, received from network,
// should be packed, should be serialized on client side
#pragma pack(push, 1)
struct settings
{
	int reading_speed_kbit_s;
	int fragment_size;
	char filename[20];	// limit filename in 20 bytes for simplicity
};
#pragma pack(pop)

struct write_thread_data
{
	struct settings *settings_ptr;
	ofstream f_write;
	string filename;	// curr fragment filename
	int len;			// last_readed data cntr
	char *buf;			// buf to read
};

struct manage_thread_data
{
	struct settings *settings_ptr;
	pthread_t *reading_th_ptr;
};

const static char *rw_pipe_name = "rw_pipe";
const static char *translation_pipe_name = "translation_pipe";

static atomic<int> rw_status;
static atomic<int> translation_status;

static mutex m_file_wr;

static int write_to_file(write_thread_data *data_p)
{
	int res = 0;

	data_p->f_write.open(data_p->filename, ios_base::binary);
	if(!data_p->f_write)
	{
		cerr << "file open error: " << strerror(errno) << endl;
		res = -1;
	}

	m_file_wr.lock();
	data_p->f_write.write(data_p->buf, data_p->len);
	data_p->f_write.close();
	m_file_wr.unlock();
	data_p->len = 0;

	return res;
}


int handle_fragment(struct write_thread_data *data_p, uint seg_num)
{
	int res = 1;
	int flags;
	int rw_pipe;
	string segment = "segment";
	data_p->filename = segment + to_string(seg_num);
	fd_set set;
	struct timeval timeout;

	flags = O_RDONLY | O_NONBLOCK;
	rw_pipe = open(rw_pipe_name, flags);

	while(1)
	{
		if (rw_pipe == -1) {
			cerr << "pipe open error" << strerror(errno) << endl;
			res = -1;
			break;
		}

		data_p->len = 0;
		while(data_p->len < data_p->settings_ptr->fragment_size && res > 0)
		{
			FD_ZERO(&set);
			FD_SET(rw_pipe, &set);

			timeout.tv_sec = 1;
			timeout.tv_usec = 0;

			res = select(rw_pipe + 1, &set, NULL, NULL, &timeout);

			if (res == 0)
			{
				// timeout
			}
			else if (res < 0)
			{
				cerr << "select error: " << strerror(errno) << endl;
			}
			else
			{
				while(data_p->len < data_p->settings_ptr->fragment_size && res > 0)
				{
					res = read(rw_pipe, &data_p->buf[data_p->len], 1);
					if(res > 0)
					{
						data_p->len += res;
					}
				}
				if(data_p->len < data_p->settings_ptr->fragment_size &&
						rw_status != READ_ENDED)
				{
					// goto select()
					res = 1;
				}
			}
		}
		close(rw_pipe);

		if(data_p->len > 0)
		{
			write_to_file(data_p);
		}
		else
		{
			res = -1;
			break;
		}

		if(!data_p->f_write)
		{
			cerr << "writing error: " << strerror(errno) << endl;
			res = -1;
		}
		else
		{
			res = 0;
		}

		break;
	}

	return res;
}


void *thread_writing(void *data) {
	struct write_thread_data *data_p = (struct write_thread_data *)data;
	int res;
	int seg_num = 0;

	while(1)
	{
		res = handle_fragment(data_p, seg_num);
		if(res < 0)
		{
		}
		else
		{
			cout << "fragment handled ok" << endl;
			seg_num++;
		}
		if(rw_status == READ_ENDED)
		{
			// read last part of file, then exit
			while(handle_fragment(data_p, seg_num) == 0)
			{
				seg_num++;
			}

			rw_status = READ_END_HANDLED;
			break;
		}
	}

	pthread_exit(NULL);
}


void *thread_reading(void *settings_ptr) {
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

	int res;

	int rw_pipe = open(rw_pipe_name, O_WRONLY );
	if (rw_pipe == -1) {
		cerr << "rw_pipe error: " << strerror(errno) << endl;
	}

	int translation_pipe = open(translation_pipe_name, O_RDWR | O_NONBLOCK );
	if (translation_pipe == -1) {
		cerr << "translation_pipe_name error: " << strerror(errno) << endl;
	}

	struct settings * settings = (struct settings *)settings_ptr;

	float bytes_per_ms = (float)settings->reading_speed_kbit_s * 1024.0 / (1000.0 * 8.0);
	float time_window_ms = DATA_SEGMENT_SIZE_READ / bytes_per_ms;
	auto timeWindow = chrono::milliseconds((int)time_window_ms); // little inaccuracy here, todo: fix

	ifstream f_read;
	f_read.open (settings->filename, ios_base::binary);

	char temp;
    char *buf = (char *)malloc(DATA_SEGMENT_SIZE_READ);
    if(buf == NULL)
	{
		cerr << "memory allocation error. error: " << strerror(errno) << endl;
		pthread_exit(NULL);
	}

    pthread_cleanup_push(free, buf);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    rw_status = READ_PROCESS;
	while(1)
	{
		auto start = chrono::steady_clock::now();

		f_read.read(buf, DATA_SEGMENT_SIZE_READ);

	    if (f_read)
		{
	    	// send readed data to wr thread
			res = write(rw_pipe, buf, f_read.gcount());
			if(res < 0)
			{
				cerr << "can't write readed data to rw_pipe. error: " << strerror(errno) << endl;
			}
	    	cout << "all characters read successfully." <<  f_read.gcount() << endl;

	    	// send readed data to translation thread
	    	if(translation_status == TRANSLATION_NONE)
	    	{
	    		// flush the pipe if we don't have client now
	    		while(read(translation_pipe, &temp, 1));
	    	}
			res = write(translation_pipe, buf, f_read.gcount());
			if(res < 0)
			{
				cerr << "can't write readed data to translation_pipe. error: " << strerror(errno) << endl;
			}
	    	cout << "all characters read successfully." <<  f_read.gcount() << endl;
		}
		else
		{
			cout << "error: only " << f_read.gcount() << " could be read" << endl;

			// access to rw_status will be protected w mem barrier
			rw_status = READ_ENDED;

			// wait for last fragment write ended
			if(f_read.gcount() > 0)
			{
				res = write(rw_pipe, buf, f_read.gcount());
				while(rw_status != READ_END_HANDLED)
				{
					this_thread::sleep_for(chrono::milliseconds(10));
				}
			}
			break;
		}

		auto end = chrono::steady_clock::now();
		auto elapsed = end - start;
		auto timeToWait = timeWindow - elapsed;
		this_thread::sleep_for(timeToWait);
	}

	pthread_cleanup_pop(1);
	pthread_exit(NULL);
}

void *thread_translation(void *params) {
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

	int listenfd = 0, connfd = 0;
	int option = 1;
	struct sockaddr_in serv_addr, client_addr;
	int n;
	int c;
	int res;
	int readed;
	fd_set set;
	struct timeval timeout;

	char temp;
    char *buf = (char *)malloc(DATA_SEGMENT_SIZE_READ);
    if(buf == NULL)
    {
    	cerr << "memory allocation error. error: " << strerror(errno) << endl;
    	pthread_exit(NULL);
    }

    pthread_cleanup_push(free, buf);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	while(1)
	{
		int translation_pipe = open(translation_pipe_name, O_RDONLY | O_NONBLOCK);
		if (translation_pipe == -1) {
			cerr << "translation_pipe error: " << strerror(errno) << endl;
			break;
		}

		listenfd = socket(AF_INET, SOCK_STREAM, 0);
		if (listenfd < 0)
		{
			cerr << "can't create socket. error: " << strerror(errno) << endl;
			res = -1;
			break;
		}
		setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));

		memset(&serv_addr, '0', sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
		serv_addr.sin_port = htons(TRANSLATION_PORT);

		if( bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
		{
			cerr << "bind failed. error: " << strerror(errno) << endl;
			res = -1;
			break;
		}

		// one client
		listen(listenfd, 1);
		c = sizeof(struct sockaddr_in);

		while(1)
		{
			cout << "waiting for client..." << endl;
			connfd = accept(listenfd, (struct sockaddr*)&client_addr, (socklen_t*)&c);
			if (connfd < 0)
			{
				cerr << "accept failed. error: " << strerror(errno) << endl;
				res = -1;
				break;
			}

			cout << "conn established..." << endl;
			translation_status = TRANSLATION_IN_PROCESS;

			// send readed fragments to client
			while(1)
			{
				FD_ZERO(&set);
				FD_SET(translation_pipe, &set);

				timeout.tv_sec = 1;
				timeout.tv_usec = 0;

				res = select(translation_pipe + 1, &set, NULL, NULL, &timeout);

				if (res == 0)
				{
					// timeout
				}
				else if (res < 0)
				{
					cerr << "select error: " << strerror(errno) << endl;
				}
				else
				{
					readed = read(translation_pipe, buf, DATA_SEGMENT_SIZE_READ);
					if(readed == DATA_SEGMENT_SIZE_READ)
					{
						n = write(connfd, buf, DATA_SEGMENT_SIZE_READ);

						if(n == 0)
						{
							cout << "client disconnected" << endl;
							translation_status = TRANSLATION_NONE;
							res = -1;
							while(read(translation_pipe, &temp, 1)); // flush pipe
							break; // goto accept()
						}
					}
					else
					{
						cerr << "error reading from translation pipe. error: " << strerror(errno) << endl;
					}
				}
			}
		}

		break;
	}

	close(listenfd);
	close(connfd);
	pthread_cleanup_pop(1);
	pthread_exit(NULL);
}

int get_params(struct settings *settings_p)
{
    int listenfd = 0, connfd = 0;
    int option = 1;
    struct sockaddr_in serv_addr, client_addr;
    int n;
    int c;
    int res;

    while(1)
	{
		listenfd = socket(AF_INET, SOCK_STREAM, 0);
		if (listenfd < 0)
		{
			cerr << "can't create socket. error: " << strerror(errno) << endl;
			res = -1;
			break;
		}
		setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));

		memset(&serv_addr, '0', sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
		serv_addr.sin_port = htons(SETTINGS_PORT);

		if( bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
		{
			cerr << "bind failed. error: " << strerror(errno) << endl;
			res = -1;
			break;
		}

		// one client
		listen(listenfd, 1);
		c = sizeof(struct sockaddr_in);

    	cout << "waiting for settings..." << endl;
        connfd = accept(listenfd, (struct sockaddr*)&client_addr, (socklen_t*)&c);
        if (connfd < 0)
		{
        	cerr << "accept failed. error: " << strerror(errno) << endl;
        	res = -1;
			break;
		}

        cout << "conn established..." << endl;

        // not perfectly serializing, but economy my time
        // should be reworked in production
        // todo: fix this
        n = recv(connfd, reinterpret_cast<char*>(settings_p), sizeof(settings), 0);

        if(n == 0)
        {
        	cout << "client disconnected" << endl;
        	res = -1;
        }
		else if(n < 0)
		{
			cerr << "recv failed. error: " << strerror(errno) << endl;
			res = -1;
		}
		else if(n == sizeof(settings))
		{
			cout << "settings readed successful " << endl;
			res = 0;
		}
        break;
    }

    close(listenfd);
    close(connfd);
    return res;
}

void *thread_manage(void *data) {
	struct manage_thread_data *manage_thread_data = (struct manage_thread_data *)data;

	while(1)
	{
		if(get_params(manage_thread_data->settings_ptr) == 0)
		{
			pthread_cancel(*manage_thread_data->reading_th_ptr);
		}
	}
}

int main()
{
	struct settings settings;
	struct manage_thread_data manage_thread_data;

	pthread_t writing_th;
	pthread_t reading_th;
	pthread_t manage_th;
	pthread_t translation_th;

	manage_thread_data.settings_ptr = &settings;
	manage_thread_data.reading_th_ptr = &reading_th;

	write_thread_data data;
	data.settings_ptr = &settings;

	while(get_params(&settings) != 0)
	{
	}

	if (pthread_create(&manage_th, NULL, thread_manage, (void*)&manage_thread_data) != 0)
		cerr << "thread create failed. error: " << strerror(errno) << endl;

	while(1)
	{
		rw_status = READ_NONE;
		translation_status = TRANSLATION_NONE;

		mkfifo(rw_pipe_name, 0666);
		mkfifo(translation_pipe_name, 0666);
		data.buf = (char *)malloc(settings.fragment_size);
		data.len = 0;

		if (pthread_create(&translation_th, NULL, thread_translation, NULL) != 0)
			cerr << "thread create failed. error: " << strerror(errno) << endl;

		if (pthread_create(&reading_th, NULL, thread_reading, (void*)&settings) != 0)
			cerr << "thread create failed. error: " << strerror(errno) << endl;

		if (pthread_create(&writing_th, NULL, thread_writing, (void*)&data) != 0)
			cerr << "thread create failed. error: " << strerror(errno) << endl;

		pthread_join(reading_th, NULL);

		sleep(1);	// spike. writing_th should end his work. todo: fix this
		m_file_wr.lock();
		pthread_cancel(writing_th);
		m_file_wr.unlock();

		pthread_cancel(translation_th);

		free(data.buf);
		unlink(rw_pipe_name);
		unlink(translation_pipe_name);

		if(rw_status == READ_END_HANDLED)
		{
			// file is over
			break;
		}
	}

	pthread_cancel(manage_th);

	return 0;
}
